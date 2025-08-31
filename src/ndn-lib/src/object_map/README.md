# Object Map 模块

这个模块提供了对象映射的功能，包括 `SimpleObjectMap` 和 `DirObject` 的实现。

## SimpleObjectMap

`SimpleObjectMap` 是一个简单的对象映射结构，用于存储键值对关系。

### 主要特性

- 支持存储不同类型的值（Object、ObjId、ObjectJwt）
- 提供基本的 CRUD 操作
- 支持序列化和反序列化
- 包含计数器和额外信息字段

### 基本用法

```rust
use ndn_lib::object_map::{SimpleObjectMap, SimpleMapItem};
use ndn_lib::object::ObjId;

let mut map = SimpleObjectMap::new();
let obj_id = ObjId::default();

// 添加条目
map.insert("key1".to_string(), SimpleMapItem::ObjId(obj_id));

// 获取条目
if let Some(item) = map.get("key1") {
    println!("找到条目: {:?}", item);
}

// 检查长度
println!("映射包含 {} 个条目", map.len());
```

## DirObject

`DirObject` 继承自 `SimpleObjectMap`，专门用于表示目录结构。

### 继承关系

`DirObject` 通过组合的方式继承了 `SimpleObjectMap` 的所有功能：

```rust
pub struct DirObject {
    // 目录特有的字段
    pub name: String,
    pub content: String,
    pub exp: u64,
    pub meta: Option<serde_json::Value>,
    pub owner: Option<String>,
    pub create_time: Option<u64>,
    pub extra_info: HashMap<String, Value>,
    
    // 继承 SimpleObjectMap 的功能
    pub object_map: SimpleObjectMap,
}
```

### 主要特性

- 继承 `SimpleObjectMap` 的所有方法
- 提供目录特有的操作（添加文件、添加目录等）
- 支持生成目录对象ID
- 包含目录元数据（所有者、创建时间等）

### 基本用法

```rust
use ndn_lib::object_map::DirObject;
use ndn_lib::object::ObjId;

// 创建目录
let mut dir = DirObject::new("my_directory".to_string());

// 设置目录元数据
dir.owner = Some("user123".to_string());
dir.create_time = Some(1234567890);

// 添加文件
let file_obj_id = ObjId::default();
dir.add_file("document.txt".to_string(), file_obj_id);

// 添加子目录
let subdir_obj_id = ObjId::default();
dir.add_directory("subdir".to_string(), subdir_obj_id);

// 使用继承的方法
println!("目录包含 {} 个条目", dir.len());
println!("目录条目: {:?}", dir.list_entries());

// 获取文件对象ID
if let Some(obj_id) = dir.get_file("document.txt") {
    println!("文件对象ID: {:?}", obj_id);
}

// 生成目录对象ID
let (obj_id, content) = dir.gen_obj_id();
```

### 委托方法

`DirObject` 提供了对 `SimpleObjectMap` 方法的委托：

- `len()` - 获取条目数量
- `is_empty()` - 检查是否为空
- `get(key)` - 获取条目
- `insert(key, value)` - 插入条目
- `remove(key)` - 删除条目
- `contains_key(key)` - 检查是否包含键
- `keys()` - 获取所有键
- `values()` - 获取所有值
- `iter()` - 迭代所有条目

### 目录特有方法

- `add_file(name, obj_id)` - 添加文件
- `add_directory(name, dir_obj_id)` - 添加子目录
- `get_file(name)` - 获取文件对象ID
- `list_entries()` - 列出所有条目
- `is_file(name)` - 检查是否为文件
- `is_directory(name)` - 检查是否为目录

## 设计模式

这个实现使用了**组合模式**而不是传统的继承，因为 Rust 不支持结构体继承。通过以下方式实现：

1. **组合**：`DirObject` 包含一个 `SimpleObjectMap` 字段
2. **委托**：`DirObject` 的方法委托给内部的 `SimpleObjectMap`
3. **扩展**：`DirObject` 添加了目录特有的字段和方法

这种设计的优势：

- 类型安全
- 清晰的职责分离
- 易于测试和维护
- 符合 Rust 的设计哲学

## 测试

运行测试：

```bash
cargo test --package ndn-lib object_map
```

测试包括：

- `SimpleObjectMap` 的基本功能测试
- `DirObject` 的创建和操作测试
- 继承关系的验证测试
- 示例代码的功能测试
