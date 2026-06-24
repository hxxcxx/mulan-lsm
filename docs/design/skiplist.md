# SkipList — 跳表双"跳"不能提前命中

**`find_prev`：所有操作的基石**

```rust
fn find_prev(&self, key: &K) -> Vec<usize> {
    for level in (0..self.max_height).rev() {
        while self.nodes[cur].next[level] != NIL
            && self.nodes[self.nodes[cur].next[level]].key < *key  // 严格 <
        {
            cur = self.nodes[cur].next[level];
        }
        prev[level] = cur;
    }
    prev
}
```

返回 `prev[i]` = 第 i 层上 **key 严格小于 target 的最后一个节点**。三个方法依赖它：

| 方法 | 怎么用 `prev[0]` |
|------|------------------|
| `get` | 看 `prev[0]` 的后继 key 是否 `==` target |
| `lower_bound` | 直接返回 `prev[0]` 的后继（第一个 ≥ target） |
| `insert` | 在 `prev[0..height]` 每一层缝合新节点 |

**为什么高层不直接判断相等就返回？**

`get` 的循环条件只用 `<`（严格小于），不用 `<=` 也不判断 `==`：

```rust
for level in (0..self.max_height).rev() {
    while self.nodes[cur].next[level] != NIL
        && self.nodes[self.nodes[cur].next[level]].key < *key
    {
        cur = self.nodes[cur].next[level];
    }
}
let next = self.nodes[cur].next[0];
if next != NIL && self.nodes[next].key == *key { ... }
```

即使在高层次"路过"了目标 key，也不停下来。因为**节点不一定在高层出现**——大部分节点只有 1~2 层。全部降到第 0 层，因为所有节点一定在第 0 层出现。

**严格 `<` 的意义**：插入重复 key（同 user_key 的多版本）时，新节点插在已有同 key 节点之前。后插入的（大 seq）排在前面，`get` / `lower_bound` / `iter` 都先碰到最新版本。

**arena 索引方案**：所有节点存于 `Vec<Node>`，节点间指针是 `usize` 索引。零 `unsafe`，比 `Box` / `Rc` 方案简单且无所有权冲突。
