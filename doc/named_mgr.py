def set_obj(objid,obj_data):
    pass

def mart_used(objid,version):
    obj = get_obj(objid)
    if obj.is_container:
        for child in obj.get_all_child():
            mark_used(child)
    
    obj.used = version


def do_gc():
    snapshot = create_snapshot()
    for path in snapshot.get_all_path():
        mark_used(path.objid,snapshot.version)

    # delete unused objs
    delete_unused_objs()

# 对上述流程进行优化的根本逻辑，是能更高效的处理某个GC完成之后的新增对象,这样可以实现分段GC
#-------------------------

def link_obj(objid):
    obj_record,obj = db.get_obj_record(objid)
    obj_record.link_count += 1
    if obj is None:
        return
    if obj.is_container:
        ref_cacl_queue.append(path,objid,1)


def link_obj_by_refcount(objid,refcount):
    obj_record,obj = db.get_obj_record(objid)
    obj_record.link_count += refcount
    if obj is None:
        return
    if obj.is_container:
        ref_cacl_queue.append(path,objid,refcount)

def unlink_obj(objid):
    obj_record,obj = db.get_obj_record(objid)
    obj_record.link_count -= 1

    if obj is None:
        return

    if obj.is_container:
        for child in obj.get_all_child():
            unlink_obj(child)

def set_path(path,objid):
    old_objid = db.set_path(path,objid)
    if old_objid:
        unlink_obj(old_objid)
    if objid:
        link_obj(objid)

def set_obj(objid,obj_data):
    obj_record = db.get_obj_record(objid)
    if obj_record:
        if obj_record.link_count > 0:
            link_obj_by_refcount(objid,obj_record.link_count)
    
    db.set_obj(objid,obj_data)


def do_gc():
    snapshot = create_snapshot()
    for objid in snapshot.get_all_obj_refcount_is_zero():
        delete_obj(objid)

# 引用计数法的逻辑就是在linkobj / unlinkobj 的时候，更新引用计数，这样在gc的时候，只需要删除引用计数为0的对象就好了
# 这里优化的重点是如何提高linkobj / unlinkobj 的时候，更新引用计数的效率，这里遍历对象的次数比标记法似乎要更多


# 结论
# 从文件系统GC的角度来看，每次运行标记，都相当于遍历所有的有效对象，这对一个空间已经充分利用，并有大量对象的系统来说，是很浪费的
# 而应用计数的遍历主要发生在Link和Unlink的时候，文件系统里的大部分对象只会被添加/删除一次，并不需要全量遍历，所以长期来看，性能损耗更小
# 可以通过将大对象refcount计算的队列化，来让系统能有更小的添加/删除的响应时间。