## 正确的接口设计

- 语义:添加展开symbolic link的次数参数，全部在客户端展开容易带来RPC风暴
- 返回的应该是DentryItem + inner_path的组合，客户端根据返回值进一步处理


## 展开symbolic link的逻辑

- 为了防止潜在的循环，展开的过程中要记录展开symbolic_link的次数，0次为不展开
- 即使服务器只支持0次展开，客户端也可使用下面逻辑手工展开(RPC调用增大)

```python
# 返回 (DentryItem,inner_path)
def reslove_path(parent_node,path_compoents,sym_count):
    current_name = path_compoents.pop()
    current_entry = get_dentry(parent_node,current_name)
    is_last = path_compoents.size() is 0
    match current_entry:
        case IndexNode(inode):
            current_node = get_inode(inode)?;
            if !is_last:
                return reslove_path(current_node,path_compoents,sym_count)
            else:
                result = create_result_by_inode(inode)
                return (result,None)
        case SYM_LINK(target_path):
            # 看看是否还能展开，不能展开就返回SYM_LINK的信息,以及剩余的compoents
            if sym_count > 0:
                new_path_compoents = normailze(target_path,path_compoents)
                return reslove_path(get_root_node(),new_path_compoents,sym_count-1)
            else:
                result = create_result_by_inode(inode)
                return (result,path_compoents)
        case ObjId(objid):
            # 返回Objid + 剩余的Componets(inner_path)
            result = create_result_by_objid(objid)
            return (result,path_compoents);    
        case Tombstone:
            return Error(NotDir)    

```

### 在上述流程上看cache的管理

- $objid/innerpath的cache,由store_mgr管理,fsmeta不管
- resolve path cache有两种：
  - path终点cache(比这个深度更大的path,肯定有inner_path)
  - inode path cache,可以看到路径上所有的inode_id,这不是终点

使用cache:
path匹配到了一个终点路径，并且还有剩余：立刻返回
path匹配到了一个非终点路径，其还有剩余，需要继续resolve_next
path是一个cache item的前缀，使用cache item (这个查询应该做优化)

更新cache
更深的路径，会覆盖更短的路径
每次resolve的最终结果，进行一次cache更新（尽量用最长的路径去写cache）


cache的失效：
  - 路径上的dentry item改变了，会失效 ： 比如 /dir1/dir2 的的dentry_item变了，所有以/dir/dir2 开头的cache item都要失效
    update_dentry_item的时候，知道路径么？
  - cache item不会因为inode的状态变化而失效



