#!/usr/bin/env python3
# coding=utf-8
"""
MPMS压力测试运行脚本
提供不同级别的测试选项和详细报告
"""

import os
import sys
import time
import subprocess
import argparse
import logging
from pathlib import Path
import json
from typing import Dict, List, Any

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 测试配置
STRESS_TEST_CONFIGS = {
    'quick': {
        'description': '快速压力测试（适合开发阶段）',
        'files': ['test_stress_comprehensive.py::TestMPMSStress::test_edge_cases',
                 'test_performance_benchmark.py::TestMPMSPerformance::test_baseline_performance'],
        'timeout': 300,  # 5分钟
    },
    'standard': {
        'description': '标准压力测试（适合CI/CD）',
        'files': ['test_stress_comprehensive.py',
                 'test_performance_benchmark.py::TestMPMSPerformance::test_baseline_performance',
                 'test_performance_benchmark.py::TestMPMSPerformance::test_scaling_performance'],
        'timeout': 1800,  # 30分钟
    },
    'intensive': {
        'description': '密集压力测试（适合发布前）',
        'files': ['test_stress_comprehensive.py',
                 'test_performance_benchmark.py',
                 'test_extreme_scenarios.py'],
        'timeout': 3600,  # 1小时
    },
    'extreme': {
        'description': '极端压力测试（适合长期稳定性测试）',
        'files': ['test_stress_comprehensive.py',
                 'test_performance_benchmark.py',
                 'test_extreme_scenarios.py'],
        'timeout': 7200,  # 2小时
        'extra_args': ['--stress-multiplier=2']
    }
}


class StressTestRunner:
    """压力测试运行器"""
    
    def __init__(self, test_level: str = 'standard'):
        self.test_level = test_level
        self.config = STRESS_TEST_CONFIGS[test_level]
        self.results = {}
        self.start_time = None
        self.end_time = None
        
        # 确保在tests目录下运行
        self.tests_dir = Path(__file__).parent
        os.chdir(self.tests_dir)
        
        logger.info(f"初始化压力测试运行器 - 级别: {test_level}")
        logger.info(f"配置: {self.config['description']}")
        
    def run_single_test(self, test_file: str, timeout: int = None) -> Dict[str, Any]:
        """运行单个测试文件"""
        logger.info(f"开始运行测试: {test_file}")
        
        # 构建命令，忽略pytest.ini中的coverage配置
        cmd = [
            'timeout', f'{timeout or self.config["timeout"]}s',
            'bash', '-c', 
            f'cd {self.tests_dir} && python -m pytest {test_file} -v -s --tb=short --override-ini="addopts=-ra --strict-markers --tb=short"'
        ]
        
        # 添加额外参数
        if 'extra_args' in self.config:
            cmd[-1] += ' ' + ' '.join(self.config['extra_args'])
        
        start_time = time.time()
        
        try:
            # 运行测试
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout or self.config['timeout']
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            test_result = {
                'test_file': test_file,
                'duration': duration,
                'return_code': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'success': result.returncode == 0,
                'timeout_occurred': False
            }
            
            if result.returncode == 0:
                logger.info(f"✅ 测试通过: {test_file} ({duration:.1f}秒)")
            else:
                logger.error(f"❌ 测试失败: {test_file} ({duration:.1f}秒)")
                logger.error(f"错误输出: {result.stderr[:500]}...")
                
        except subprocess.TimeoutExpired:
            end_time = time.time()
            duration = end_time - start_time
            
            test_result = {
                'test_file': test_file,
                'duration': duration,
                'return_code': -1,
                'stdout': '',
                'stderr': f'测试超时 ({timeout}秒)',
                'success': False,
                'timeout_occurred': True
            }
            
            logger.error(f"⏰ 测试超时: {test_file} ({duration:.1f}秒)")
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            test_result = {
                'test_file': test_file,
                'duration': duration,
                'return_code': -2,
                'stdout': '',
                'stderr': str(e),
                'success': False,
                'timeout_occurred': False
            }
            
            logger.error(f"💥 测试异常: {test_file} - {e}")
        
        return test_result
    
    def run_all_tests(self) -> Dict[str, Any]:
        """运行所有测试"""
        logger.info(f"开始运行 {self.test_level} 级别的压力测试")
        logger.info(f"预计运行时间: {self.config['timeout']} 秒")
        
        self.start_time = time.time()
        
        # 运行每个测试文件
        for test_file in self.config['files']:
            self.results[test_file] = self.run_single_test(test_file)
            
            # 如果是严重失败，考虑是否继续
            if not self.results[test_file]['success'] and self.results[test_file]['return_code'] < 0:
                logger.warning(f"严重失败，继续下一个测试...")
        
        self.end_time = time.time()
        
        # 生成报告
        return self.generate_report()
    
    def generate_report(self) -> Dict[str, Any]:
        """生成测试报告"""
        total_duration = self.end_time - self.start_time
        successful_tests = sum(1 for r in self.results.values() if r['success'])
        total_tests = len(self.results)
        
        report = {
            'test_level': self.test_level,
            'total_duration': total_duration,
            'total_tests': total_tests,
            'successful_tests': successful_tests,
            'failed_tests': total_tests - successful_tests,
            'success_rate': successful_tests / total_tests if total_tests > 0 else 0,
            'test_results': self.results,
            'summary': self._generate_summary()
        }
        
        return report
    
    def _generate_summary(self) -> str:
        """生成测试摘要"""
        total_tests = len(self.results)
        successful_tests = sum(1 for r in self.results.values() if r['success'])
        failed_tests = total_tests - successful_tests
        total_duration = self.end_time - self.start_time
        
        summary_lines = [
            f"压力测试摘要 - {self.test_level.upper()} 级别",
            "=" * 50,
            f"总测试数: {total_tests}",
            f"成功: {successful_tests}",
            f"失败: {failed_tests}",
            f"成功率: {(successful_tests/total_tests*100):.1f}%" if total_tests > 0 else "成功率: N/A",
            f"总耗时: {total_duration:.1f} 秒",
            "",
            "详细结果:"
        ]
        
        for test_file, result in self.results.items():
            status = "✅" if result['success'] else "❌"
            duration = result['duration']
            summary_lines.append(f"  {status} {test_file} ({duration:.1f}s)")
            
            if not result['success']:
                error_msg = result['stderr'][:100] + "..." if len(result['stderr']) > 100 else result['stderr']
                summary_lines.append(f"      错误: {error_msg}")
        
        return "\n".join(summary_lines)
    
    def save_report(self, report: Dict[str, Any], output_file: str = None) -> str:
        """保存测试报告"""
        if output_file is None:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_file = f"stress_test_report_{self.test_level}_{timestamp}.json"
        
        output_path = self.tests_dir / output_file
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"测试报告已保存到: {output_path}")
        return str(output_path)
    
    def print_summary(self, report: Dict[str, Any]):
        """打印测试摘要"""
        print("\n" + report['summary'])
        
        # 额外的统计信息
        if report['failed_tests'] > 0:
            print(f"\n⚠️  有 {report['failed_tests']} 个测试失败，请查看详细日志")
        
        if report['success_rate'] >= 0.9:
            print("\n🎉 压力测试整体表现优秀！")
        elif report['success_rate'] >= 0.7:
            print("\n👍 压力测试表现良好")
        elif report['success_rate'] >= 0.5:
            print("\n⚠️  压力测试表现一般，需要关注")
        else:
            print("\n🚨 压力测试表现较差，需要紧急处理")


def check_dependencies():
    """检查依赖"""
    missing_deps = []
    
    try:
        import psutil
    except ImportError:
        missing_deps.append('psutil')
    
    try:
        import pytest
    except ImportError:
        missing_deps.append('pytest')
    
    if missing_deps:
        logger.error(f"缺少依赖: {', '.join(missing_deps)}")
        logger.error("请运行: pip install " + " ".join(missing_deps))
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='MPMS压力测试运行器')
    parser.add_argument(
        '--level', 
        choices=['quick', 'standard', 'intensive', 'extreme'],
        default='standard',
        help='测试级别 (默认: standard)'
    )
    parser.add_argument(
        '--output', 
        help='报告输出文件名'
    )
    parser.add_argument(
        '--list-levels', 
        action='store_true',
        help='列出所有测试级别'
    )
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='只显示将要运行的测试，不实际执行'
    )
    
    args = parser.parse_args()
    
    if args.list_levels:
        print("可用的测试级别:")
        for level, config in STRESS_TEST_CONFIGS.items():
            print(f"  {level}: {config['description']}")
            print(f"    预计时间: {config['timeout']} 秒")
            print(f"    测试文件: {', '.join(config['files'])}")
            print()
        return
    
    # 检查依赖
    check_dependencies()
    
    # 创建运行器
    runner = StressTestRunner(args.level)
    
    if args.dry_run:
        print(f"将要运行的测试 ({args.level} 级别):")
        for test_file in runner.config['files']:
            print(f"  - {test_file}")
        print(f"预计总时间: {runner.config['timeout']} 秒")
        return
    
    try:
        # 运行测试
        report = runner.run_all_tests()
        
        # 保存报告
        report_file = runner.save_report(report, args.output)
        
        # 显示摘要
        runner.print_summary(report)
        
        # 返回适当的退出码
        if report['success_rate'] >= 0.7:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("测试被用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"运行测试时发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 