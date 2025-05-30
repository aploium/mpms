#!/usr/bin/env python3
# coding=utf-8
"""
测试 MPMS iter_results 功能
"""

from mpms import MPMS


def simple_worker(index):
    """简单的工作函数"""
    if index == 5:
        raise ValueError(f"任务 {index} 出错了")
    return index * 2


def test_iter_results():
    """测试 iter_results 基本功能"""
    print("测试 iter_results 功能...")
    
    # 创建 MPMS 实例，不使用 collector
    m = MPMS(simple_worker, processes=2, threads=2)
    m.start()
    
    # 提交任务
    for i in range(10):
        m.put(i)
    
    # 关闭任务队列
    m.close()
    
    # 收集结果
    results = []
    errors = []
    
    # 使用 iter_results 获取结果
    for meta, result in m.iter_results():
        if isinstance(result, Exception):
            errors.append((meta.args[0], str(result)))
            print(f"任务 {meta.args[0]} 失败: {result}")
        else:
            results.append((meta.args[0], result))
            print(f"任务 {meta.args[0]} 成功: {result}")
    
    # 等待所有进程结束
    m.join(close=False)
    
    # 验证结果
    print(f"\n成功任务数: {len(results)}")
    print(f"失败任务数: {len(errors)}")
    
    # 检查结果是否正确
    assert len(results) == 9, f"应该有9个成功任务，实际有{len(results)}个"
    assert len(errors) == 1, f"应该有1个失败任务，实际有{len(errors)}个"
    
    # 检查失败的任务是否是任务5
    assert errors[0][0] == 5, f"失败的任务应该是任务5，实际是任务{errors[0][0]}"
    
    # 检查成功任务的结果
    for task_id, result in results:
        expected = task_id * 2
        assert result == expected, f"任务{task_id}的结果应该是{expected}，实际是{result}"
    
    print("\n✅ 所有测试通过！")


def test_iter_results_vs_collector():
    """测试不能同时使用 iter_results 和 collector"""
    print("\n测试 iter_results 和 collector 的互斥性...")
    
    def dummy_collector(meta, result):
        pass
    
    # 创建带 collector 的 MPMS
    m = MPMS(simple_worker, collector=dummy_collector)
    m.start()
    m.put(1)
    m.close()
    
    # 尝试使用 iter_results，应该抛出异常
    try:
        for _ in m.iter_results():
            pass
        assert False, "应该抛出 RuntimeError"
    except RuntimeError as e:
        print(f"✅ 正确抛出异常: {e}")
    
    m.join(close=False)


if __name__ == '__main__':
    test_iter_results()
    test_iter_results_vs_collector()
    print("\n所有测试完成！") 