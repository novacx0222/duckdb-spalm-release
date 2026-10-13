# force-method：强制方法与计时说明

本次仅修改主仓库 `dev/force-method` 和它使用的 SPALM 库，尚未合并到 hash/sort。
这是 no-encoder 基线；不增加编码算法，不将不存在的编码步骤输出为虚假的零耗时。

## 输出约定

所有自定义时间行都是 `[阶段名称] 123.456 ms`，保留三位小数。
`1` 是 DuckDB sink/RHS 侧，`2` 是 operator/LHS 侧，不依赖 SQL 中 A/B 的表面顺序。
维度、nnz 和选择的执行方法仍作为非计时信息输出。
DuckDB 自带 EXPLAIN ANALYZE 的 `Total Time` / 算子 profiler 格式不属于这套自定义计时，未修改。

- `total`：阶段整体实际经过时间，包含子阶段，不与子阶段重复相加。
- `cumulative`：从该侧 slice 创建起点累计到某个节点，保留旧节点覆盖范围。
- `worker-sum`：并行线程各自经过时间之和，可能大于壁钟时间，不能与壁钟相加。
- 其余名称：对应子阶段实际经过时间；仍可能嵌套在某个 total 中。

## 两侧输入准备

| 节点 | 含义 | 父阶段 |
|---|---|---|
| `[1-collect-input]` | 从 sink 准备开始，到所有输入被收集、合并后进入 Finalize | 1-prepare-total |
| `[2-collect-input]` | 从第一侧完成后重置时钟，到最后一个 operator 收集/合并完数据 | 2-prepare-total |
| `[1/2-combine-input]` | 实际 Combine 调用耗时之和；在锁内串行累计，不包含获取锁的等待 | collect-input |
| `[1/2-dimension-scan]` | 遍历数据推导最大行号和 join key，包括本阶段 scan chunk 准备 | prepare-total |
| `[1/2-csr-allocate]` | CSR row pointer、index、value 数组分配及初始化 | prepare-total |
| `[1/2-scan-chunks-prepare]` | 初始化 BuildCSR 所需的各线程 DataChunk | prepare-total |
| `[1/2-csr-histogram-allocate]` | 分配、清零各线程行计数数组 | csr-count-total |
| `[1/2-csr-row-count]` | 并行获取 chunk、提取行号并统计行非零数 | csr-count-total |
| `[1/2-csr-prefix-offset]` | 汇总行计数、前缀和、分配并填充 thread offset | csr-count-total |
| `[1/2-csr-count-total]` | 原 CSR offset counting，包含上述三个阶段 | prepare-total |
| `[1/2-csr-write]` | 第二次扫描，读取行号、join key、数值并填入 CSR | prepare-total |
| `[1/2-csr-sort]` | 每行按列编号排序；包含各行排序临时空间 | prepare-total |
| `[1/2-array-extract-worker-sum]` | 两次 CSR 扫描的 FetchChunk / GetData 时间跨线程累加；基线直接使用 typed array，不单独构造编码数组 | csr-row-count / csr-write 内部 |
| `[1/2-prepare-total]` | 保留原 1/2 CSR init 的完整计时范围 | prepare-total |

第二侧还包含以下一次性结果数组节点：

- `[result-allocate]`：分配结果数组。DuckDB 的 make_uniq_array 可能同时进行值初始化，所以不是纯 malloc 时间。
- `[result-zero]`：显式 memset。
- `[result-allocate-zero-total]`：保留原分配＋清零总范围，包含以上两个节点。

`[library-config-setup]` 单独记录动态库加载、读取配置、准备参数数组。
`[prepare-total]` 从第一侧计时起点到调用 SPALM 库前，包含两侧准备及库配置，不包含 MM。

## slice、MM、释放

- `[1/2-slice-subrow-csr]`、`[1/2-slice-subrow-csc]`、`[1/2-slice-csc]`：新增各步骤独立计时。
- 三个对应 `-cumulative` 节点：保留旧代码使用共同起点的累计计时。
- `[slice-prepare-total]`：两侧 slice 结构总准备时间。
- `[mm-select-total]`：cost 计算；保留原 MM check 范围。后面的分支判断开销仍在 mm-total 中。
- `[mm-total]`：原 Total MM 范围；包含后端额外准备，不仅是乘法核心。
- `[slice-mm-total]`：原外层 E2E MM，包含 slice-prepare-total 和 mm-total。
- `[mkl-baseline-total]`：原 MKL baseline 内部 E2E，和外层 slice-mm-total 区分。
- `[library-initialize]`：库入口的配置解析、阈值与 tile size 初始化，位于 slice-mm-total 之前。
- `[thresholds-deallocate]`：库出口释放阈值数组。
- `[library-call-total]`：扩展 wrapper 对完整库调用的计时，包含库内初始化、slice/MM、库调用结束前的清理。
- `[slice-deallocate]`：MM 之后释放 SPALM 的 slice 所有权数组，位于 slice-mm-total 之后。
- `[mkl-dense-input-deallocate]`：dense 输入数组和 MKL buffers 清理，仍在 mm-total 中。
- `[mkl-sparse-deallocate]`、`[mkl-bsr-deallocate]`、`[mkl-buffers-deallocate]`：相应 MKL 清理。
- `[output-total]`：保留原 Output time，从 MM 调用结束后到所有输出 source 完成。
- `[wrapper-deallocate]`：输出结束后释放 wrapper 的输入 CSR、dense 结果和 collected data。

其他 kernel 内已有计时全部保留并改名，逐节点旧→新对照在 `timing-library-mapping.json`。
原 `.s` 日志文本已移除。`print_time` 的返回值仍维持秒，避免改变潜在调用者的计算语义；只有输出单位统一成 ms。

## 旧计时对应关系

| 原名称 | 新名称 |
|---|---|
| Start to finalize time | 1-collect-input |
| 1 / 2 CSR init time | 1 / 2-prepare-total |
| CSR offset counting time | 对应侧 csr-count-total |
| CSR writing time | 对应侧 csr-write |
| CSR sorting time | 对应侧 csr-sort |
| Result memset + allocation time | result-allocate-zero-total |
| Total ss_matrix init time | slice-prepare-total |
| Subrow CSR / CSC / Slice CSC creation time | 对应侧的三个 cumulative 节点 |
| MM check time | mm-select-total |
| Total MM time | mm-total |
| 外层 E2E MM time | slice-mm-total |
| MKL 内层 E2E MM time | mkl-baseline-total |
| Output time | output-total |

## 强制方法修复

实际 dispatch 使用同一个 `method_dispatch.h` 函数：
显式 FORCE_MKL_DENSE / FORCE_MKL_SPARSE 先于自动 dense / sparse heuristic。
因此即使自动 dense 条件成立，FORCE_MKL_SPARSE 仍进入 sparse 后端。
每次加载方法时先清除旧 force flags，避免同进程的旧配置残留。
自动模式在没有 force 时维持原选择顺序。

## 后续合并到 hash/sort

保持本次 `1/2` 两侧及总阶段命名。在各 encoder 真正执行的位置新增：

- `[1/2-array-allocate]`：编码临时数组分配。
- `[1/2-array-extract]`：填入行号、join key、value 数组的独立阶段壁钟。
- `[1/2-row-encode]`：各自独立 row encoder 的 EncodeBatch。
- `[1/2-join-encode]`：共享 join encoder 的两次 EncodeBatch 调用。
- `[2-remap-first-join]`：只有实际发生时才测量重映射第一侧 join 编号。
- `[output-decode-worker-sum]`：输出解码如需单独统计，明确并行累加语义。

不要用基线的 array-extract-worker-sum 与 encoder 的 array-extract 当作完全相同范围：
前者是在 CSR 两次扫描中借用 typed arrays，后者通常是复制到编码数组。
本次不改 encoder 分支；合并时需要逐项检查冲突，不能直接拿 no-encoder physical_spalm.cpp 覆盖它们。

## 验证边界

已增加 force 与自动策略冲突的回归测试，以及毫秒输出、三位小数、旧节点保留和两侧 slice 的测试。
本地为 Apple Silicon，完整生产库需要 x86 SIMD 和 Intel MKL，因此尚未执行 Linux/MKL 完整数值集成测试。
主扩展源码做本地语法检查；SPALM dispatch 源文件做 x86 目标语法检查，其中 MKL 使用仅声明的临时头文件，不能替代真实 MKL 链接检查。
没有连接服务器，没有生成新的 benchmark 数据，不把格式示例当作实测结果。
