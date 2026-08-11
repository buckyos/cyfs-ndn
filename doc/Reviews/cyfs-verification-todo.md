# CYFS Verification TODOS

## 和去中心验证有关的核心检查点

## resove_did 的获取和验证
- [ ] discover_zone_document_from_host 根据一个zone did,获得其self-host的zone-doucment-jwt(不依赖https)。要确定URL，也要确定jwt是什么时候生成的，以及通过BNS更新后，相关zone是否有改变
- [x] 确认系统里DeviceDocument是否签名了 OK 签名过
- [ ] 确认resolve内部是否对JWT格式的did-document进行了正确的验证

## Did-Document修改
- [x] @context 增加专用schema定义 OK
- [x] 增version_seq
- [x] 在OwnerDocument中增加新鲜度的概念
- [ ] Review验证方法,kid等的使用更规范

 
## 实现传统的吊销
- [x] cache机制修改，增加新鲜度概念
- [x] cache机制修改, 优先version_seq

## On-Site密钥支持(要扩展成新动词的逻辑)
- [x] 调整OwnerDocument,支持 onsite密钥(双向认可)
- [ ] buckyos在初始化时默认创建onsite密钥，并有正确的保存区域
- [ ] 支持二级用户（非链上存储）
    reslove("did:bns:bob.zhicong","owner") ,
    返回的OwnerConfig应该有谁的签名？zone owner的签名


## 明确一些动词所需要的私钥 OK 
- 钱包 : 通常是主私钥，但可以根据不同类型的地址有不同
- 发布Zone语义路径: 当前Zone的有效私钥 （不一定是gateway device的私钥)
- 创建信息 : 当前用户的OnSite私钥
- 创建内容 : 内容Owner的主密钥
- 收款 ：通常是收款对象的Owner的主钱包地址

- Agent花零钱 : Agent的on-site密钥
- Agent收款 ： Agent的Owner钱包
- Agent创建信息（跨站用） : Agent的on-site密钥
- Agent创建内容 ： Owner的主密钥


## 去中心联合登录过程中对on-site key的应用

需求 bob已经有自己的default_zone, 然后能使用bob的身份在使用alice's zone上的app

站在alice's zone看来
- bob登录时选择联合登录
- 登录面板弹出引导，要求bob输入自己的 did （浏览器如何跨站发现”当前用户的did")
- 登录面板根据did对bob的OwnerDocument进行解析，判断是否该OwnerDocument是否可用
- 跳转到bob所在zone的登录对话框(这个对话框和上一个登录对话框的形态一定要有区别)
- bob在自己的zone里登录成功,跳转回alice'zone
- alice'zone 根据OwnerDocument里的公钥，验证带回的jwt,通过则验证成功
- alice'zone app根据bob的did,分配本zone verify-hub签发的jwt
- 相关应用服务，根据bob did所在的用户组权限，进行访问控制

核心结论: 跨zone登录，更关注OwnerDocument，而不是Owner's defualt_zone's verify-hub key (还是2个都支持？)

如果bob没有default_zone,在登录时也可以通过提供自己OwnerDocument中声明的有效公钥的签名来登录（钱包登录）

- bob登录时选择联合登录
- 登录面板弹出引导，要求bob输入自己的 did （浏览器如何跨站发现”当前用户的did")
- 登录面板根据did对bob的OwnerDocument进行解析，判断是否该OwnerDocument是否可用
- 弹出钱包登录框
- bob输入密码，签发一个登录用的jwt
- alice'zone 根据OwnerDocument里的公钥，验证带回的jwt,通过则验证成功
- alice'zone app根据bob的did,分配本zone verify-hub签发的jwt
- 相关应用服务，根据bob did所在的用户组权限，进行访问控制