# ADR-0029: VPP 风格 IP interface admin 与地址路由生命周期

Status: proposed

Date: 2026-09-19

本文定义 software-interface admin up/down 与 IPv4/IPv6 interface-address route contribution
的 owner、调用顺序、FIB/DPO 结果、具体 IP adjacency layout 和验收合同。

本文补全 ADR-0028 的地址生命周期。ADR-0028 中把 interface route contribution、
prefix pool/hash 和 IPv6 link-local table route 延期的表述由本文取代；ADR-0028 已确定的
地址 owner、错误语义、enable 引用、not-enabled Feature Node 与 tuntap callback 合同保持不变。
ADR-0005 已批准的 service FIB、`ReceiveDpo<A>`、`LoadBalanceDpo` 和具体 IPv4/IPv6
forwarding owner 边界继续有效。ADR-0005 的泛型 `AdjacencyDpo<A,R>` 决定由本文撤回：
VPP adjacency 是 IP 插件拥有的一个具体 `ip_adjacency_t` pool，不是 service 泛型载荷。

## 1. 问题与范围

`ip4_add_del_interface_address` / `ip6_add_del_interface_address` 不能只修改地址池、
enable 引用并通知 callback。vendored VPP 在 ordinary address mutation 成功后，如果
software interface 当前 admin-up，会同步调用 family-specific interface route helper；
admin 状态变化时又会遍历该 interface 的全部 ordinary addresses，成对安装或撤销相同路由。

因此 ADR-0028 当前实现不是完整的 VPP address operation：地址存在但 FIB 中没有 local host、
connected/attached prefix、glean 或 IPv4 special host route，admin up/down 也不会改变这些
contribution。

本文纳入：

1. service-owned software-interface admin-up/down callback inventory 与 dispatch；
2. private `ip4_add_interface_routes` / `ip4_del_interface_routes`；
3. private `ip6_add_interface_routes` / `ip6_del_interface_routes`；
4. ordinary address add/delete 与 admin up/down 的 route call sites；
5. `IpLookupMain` 内的 per-interface prefix pool/hash/refcount；
6. IPv4 local `/32`、connected prefix、network/broadcast special route 与 `/31` peer route；
7. IPv6 local `/128` 与 connected prefix route；
8. IPv6 link owner维护的 per-interface link-local FIB `/128` local route；
9. interface-source route 所需的 receive、glean、attached-neighbor、drop 与 load-balance
   forwarding projection；
10. 两个family FIB对SIMPLE/API/INTERFACE/ADJACENCY behavior的显式operations registration，以及
    `FIB_SOURCE_ADJ` cover/refinement FES lifecycle；
11. 只验证上述生命周期的聚焦测试。

本文不纳入：

- external route Binary API、table create/delete/bind 或 route dump；
- MFIB、IPv4/IPv6 multicast interface route；
- ARP/ND packet generation与neighbor completion、neighbor Binary API、DAD、RA、MLD；ADJ FES及
  incomplete neighbor adjacency identity仍属于本文的control-plane前置合同；
- classify table 配置。VPP `ip[46]_add_interface_routes` 的 classify branch 只有在
  `classify_table_index_by_sw_if_index` 已配置时才执行；Hammer 当前没有该 owner 或配置入口，
  本文不创建永远不可达的 classify state；
- directed-broadcast 配置 API。Hammer 当前没有对应 interface flag；本文实现 VPP 默认的
  directed-broadcast-disabled 分支，即 IPv4 subnet broadcast `/32` 为 drop + loose-uRPF-exempt；
- 邻居解析完成后的 adjacency replacement/back-walk。本文必须发布真实 glean 或 attached
  adjacency identity，不能用 drop、receive 或 interface-RX DPO 冒充，但解析它的 ARP/ND owner
  由后续 ADR 完成。

## 2. VPP 源码证据

| ID | vendored VPP source | 源码事实 | 本文约束 |
| --- | --- | --- | --- |
| V1 | `third_party/vpp/src/vnet/ip/ip4_forward.c:609-788`, `ip4_add_del_interface_address_internal` | ordinary address pool mutation和IP/MFIB enable之后，仅在`vnet_sw_interface_is_admin_up`为真时调用`ip4_add_interface_routes`或`ip4_del_interface_routes`，随后才调用address callbacks | Hammer ordinary IPv4 address成功路径必须保留同一条件与相对顺序；admin-down时地址仍存在但不安装route |
| V2 | `third_party/vpp/src/vnet/ip/ip6_forward.c:263-477`, `ip6_add_del_interface_address` | ordinary address mutation后执行family enable和link enable；admin-up时add/del routes；然后address callbacks；delete最后link disable | Hammer必须保留IPv6 route相对link enable、callback、link disable的顺序 |
| V3 | `third_party/vpp/src/vnet/ip/ip4_forward.c:412-460`, `ip4_add_interface_routes` | 每个地址安装interface-source local `/32`；prefix小于32时再安装prefix routes；classify是独立可选source | local host与prefix contribution都是address operation的一部分，不得延期或用地址池存在代替 |
| V4 | `third_party/vpp/src/vnet/ip/ip4_forward.c:309-409`, `ip4_add_interface_prefix_routes` | prefix key为`(network/prefix_len, sw_if_index)`；重复prefix只增加refcount；首次安装connected+attached path；`<= /30`另装network drop与broadcast route；`/31`另装peer attached host | Hammer prefix owner必须按interface计数；最终adjacency类型由path resolver和interface link facts决定，同prefix多个地址共享prefix route但各自保留local host route |
| V5 | `third_party/vpp/src/vnet/ip/ip4_forward.c:462-563`, `ip4_del_interface_prefix_routes`, `ip4_del_interface_routes` | IPv4先删除local `/32`，再减少prefix refcount；只有最后一个地址才删除special hosts与glean prefix，随后释放prefix record | delete不能按每个地址重复撤销共享prefix，也不能先删pool record后丢失route facts |
| V6 | `third_party/vpp/src/vnet/ip/ip6_forward.c:35-138`, `ip6_add_interface_prefix_routes`, `ip6_add_interface_routes` | 每个地址安装interface-source local `/128`；prefix小于128时按`(network/prefix_len, sw_if_index)`共享connected+attached path | IPv6与IPv4共享prefix owner算法和resolver分支，但没有IPv4 network/broadcast或`/31`分支 |
| V7 | `third_party/vpp/src/vnet/ip/ip6_forward.c:140-203`, `ip6_del_interface_prefix_routes`, `ip6_del_interface_routes` | IPv6 delete先减少/删除prefix route，再删除local `/128` | Hammer保留family-specific delete顺序，不用一个统一函数抹平IPv4/IPv6差异 |
| V8 | `third_party/vpp/src/vnet/ip/ip4_forward.c:843-876`, `ip4_sw_interface_admin_up_down`; `ip6_forward.c:486-518`, `ip6_sw_interface_admin_up_down` | 两个family callback都读取family FIB mapping，遍历该interface的ordinary address list；up逐个add routes，down逐个del routes | IP plugin注册两个concrete admin callbacks；不使用`IpVersion` dispatcher或`IpFamilyMain` |
| V9 | `third_party/vpp/src/vnet/interface.c:390-445`; `interface.h:239-257` | interface先写新flags，再调用按priority排序的global software-interface admin callbacks，之后才调用device/hw-class callbacks；global callback错误恢复flags | Hammer增加独立admin callback inventory并保留成功路径调用顺序；不能复用device/hw-class单一callback或software add/del inventory |
| V10 | `third_party/vpp/src/vnet/fib/fib_table.c:483-560`, `fib_table_route_path_fixup` | connected+非local且零next-hop的path fixup为attached/glean语义；local path转为receive；attached host转为attached-next-hop | Hammer必须保留path facts到resolver；`InterfaceRxDpo`改变packet RX identity并重新进input，不是receive或adjacency替代品 |
| V11 | `third_party/vpp/src/vnet/fib/fib_path.c:609-718,2120-2127` | attached-next-hop解析为neighbor adjacency；attached path再按P2P/NBMA/ordinary选择zero-neighbor/drop/glean；local path解析为receive DPO | interface-source contribution通过path/DPO owner与interface facts构建forwarding，不直接把`sw_if_index`写成FIB index或DPO index |
| V12 | `third_party/vpp/src/vnet/adj/adj_glean.c:96-147`, `adj_glean_add_or_lock` | glean identity按protocol/interface/normalized connected-prefix共享；对象仍来自全局`ip_adjacency_t` pool，`lookup_next_index=GLEAN`，subtype保存`fib_prefix_t`，rewrite保存interface与MTU | Hammer使用IP插件私有的单一`IpAdjacency` pool；glean不是独立泛型payload或独立family pool |
| V13 | `third_party/vpp/src/vnet/ip/ip4_forward.c:270-306`, `ip4_add_subnet_bcast_route` | directed broadcast关闭时broadcast `/32`是interface-source drop + loose-uRPF-exempt；开启时才使用broadcast adjacency | 本文没有directed-broadcast配置，因此只实现源码默认关闭分支并在测试中固定该事实 |
| V14 | `third_party/vpp/src/vnet/ip/ip6_link.c:139-243,260-310,340-371`; `ip6_ll_table.c:83-154` | link首次enable在per-interface link-local FIB中以`FIB_SOURCE_IP6_ND`安装`/128` local route；replace先删旧route再装新route；last lock删除route并在无entry时释放该table | `Ip6LinkMain`必须同时拥有link identity/locks和per-interface link-local table；link-local route不进入ordinary unicast table或ordinary address callback |
| V15 | `third_party/vpp/src/vnet/ip/lookup.h:45-104`, `ip_interface_prefix_t`, `ip_lookup_main_t` | prefix pool/hash与address pool同属family lookup main；prefix record保存key/refcount，IPv4还保存source address record index | `IpLookupMain`补入真实prefix owner；不能把prefix引用放进`InterfaceMain`或临时从FIB entry count推导 |
| V16 | `third_party/vpp/src/vnet/fib/fib_source.h:190-250`; `fib_source.c:65-104` | source由id、唯一priority和behavior注册；动态source由`fib_source_allocate`分配，数值更小者优先，`FIB_SOURCE_INTERFACE`为priority `0x03`、behavior `FIB_SOURCE_BH_INTERFACE` | service必须提供source/priority/behavior注册，而不是把全部source固化成Rust enum或关联常量；interface routes不能旁路winner选择 |
| V17 | `third_party/vpp/src/vnet/fib/fib_table.c:483-560,771-851`; `fib_entry_src.c:1548-1674` | ordinary route先做path fixup和排序，再以`update`/`update_one_path`替换该source的完整path set；source record保存entry flags并连接path-list | local、connected/attached和`/31`peer必须提交现有`FibPath`，不能先构造LB再把`DpoId`塞给FIB |
| V18 | `third_party/vpp/src/vnet/fib/fib_entry_src.c:1247-1445`; `fib_entry.c:770-805,823-858,1159-1210` | special add使用per-source refcount；source只在0->1建立、1->0销毁；winner变化执行deactivate/activate/reactivate，loser仍保留在entry | Hammer的live table必须用现有`FibEntrySrc`承载refcount、entry flags、path-list和source flags，不能用平行map代替 |
| V19 | `third_party/vpp/src/vnet/fib/fib_path_list.c:160-310,553-583,1195-1224`; `fib_entry_src.c:243-407` | path-list按paths与key flags共享并拥有生命周期；path解析后由contributing source收集forwarding、构造LB | Hammer必须把现有`FibPathList`接入live entry/source graph，由backend从winner path-list投影LB；forwarding trie只接收最终projection |
| V20 | `third_party/vpp/src/vnet/fib/fib_path_list.c:420-447,553-583`; `fib_entry_src.c:548-695`; `dpo/load_balance.c:337-365` | path-list烘焙uRPF，entry构造把该uRPF绑定到LB；loose-uRPF-exempt是entry-local例外；LB替换uRPF时先后维护引用 | Hammer复用现有`FibPathList::bake_urpf`、`FibUrpfList`和`set_load_balance_urpf`，但必须由live projection自动完成，不能由interface helper手工拼装 |
| V21 | `third_party/vpp/src/vnet/fib/fib_entry_src_interface.c:18-115,163-305` | interface behavior的local source跟踪less-specific connected cover；首个local地址为glean提供source address，winner/cover变化时移交；deactivate移除cover sibling | 本slice所需的interface behavior必须完成cover/sibling与`PROVIDES_GLEAN`语义，不能把cover仅当forwarding trie回退项 |
| V22 | `third_party/vpp/src/vnet/fib/fib_path.c:609-718,2120-2127` | path resolver把attached-next-hop投影为neighbor adjacency、attached path按link facts投影neighbor/drop/glean、local path投影receive DPO，并由path/adjacency依赖承接更新 | concrete IPv4/IPv6 backend必须从现有`FibPath`与interface事实解析DPO并建立依赖；route helper不直接调用DPO pool |
| V23 | `third_party/vpp/src/vnet/fib/fib_table.c:320-480,606-920`; `fib_entry_src.c:1400-1412`; `ip4_forward.c:463-491`; `ip6_forward.c:141-168` | FIB add/update返回entry index，remove/delete为void；不存在prefix的table删除为no-op，source record不存在时remove action不返回error；interface-prefix record缺失只`clib_warning`并返回 | Hammer不得给这些路径增加`FibError`、boolean missing结果、rollback、transaction或panic语义 |
| V24 | `third_party/vpp/src/vnet/fib/fib_types.h:191-230`; `vnet/adj/adj.h:216-359`; `adj.c:58-106`; `adj_nbr.c:225-251` | `fib_prefix_t`为length/protocol/pad加16-byte IP46 address，address offset为4；`ip_adjacency_t`按64字节对齐且总长4个cache line，subtype在第1条、rewrite占第2/3条、node/lookup/link/protocol/flags在第4条；neighbor和glean来自同一个pool | IP插件定义`IpFibPrefix`与`IpAdjacency`并用编译期size/alignment/offset断言固定layout；不以`IpNet` enum或service `AdjacencyDpo`充当池内对象 |
| V25 | `third_party/vpp/src/vnet/fib/fib_node.h:180-376`; `fib_node.c:20-190`; `fib_node_list.h`; `fib_node_list.c` | 每个FIB graph object嵌入12-byte `fib_node_t`；node type必须注册get/last-lock/back-walk/memory operations；child由`(node type,index)`定位并返回sibling handle；add child取得parent lock，remove child释放 | service net必须定义可注册的`FibNode`抽象和`FibNodeList`容器；插件注册自己的concrete node operations并继续拥有typed pool，不能靠IP插件私有`match FibNodeType`假装完成通用FIB graph |
| V26 | `third_party/vpp/src/vnet/adj/adj.c:256-355,558-628`; `fib_path.c:609-718,860-914,1940-1965` | adjacency lock就是embedded FIB-node lock；path解析到adjacency后以`adj_child_add(...FIB_NODE_TYPE_PATH...)`保存sibling，path销毁/重解析先remove child；last adjacency lock断言child list为空再删除DB/pool | Hammer path不能只在projection时临时创建DPO；必须成为有稳定index/sibling的node-backed对象，并在adjacency child list中建立/解除依赖 |
| V27 | `third_party/vpp/src/vnet/adj/rewrite.h:40-99`; `adj.c:410-520`; `adj_glean.c:147-244`; `adj_nbr.c:412-595` | rewrite header含interface、next、`u16 data_bytes`、MTU、flags和mcast offset，连同data固定128 bytes；MTU/MAC/rewrite/type变化在barrier内更新并按原因back-walk；glean/incomplete node index是对象事实 | `IpAdjacencyRewrite`逐字段对齐而非opaque padding；对象保存真实family node index；本slice至少完成MTU更新与path child通知，neighbor完成转换及ARP/ND packet producer明确延期 |
| V28 | `third_party/vpp/src/vnet/fib/fib_entry_src.h:25-207`; `fib_entry_src.c:20-61`; `fib_entry_src_default.c`; `fib_entry_src_interface.c` | FES是按behavior显式注册的VFT，包含init/deinit、activate/deactivate/reactivate、add/remove、path swap/add/remove、cover change/update、installed、forwarding update、source data及copy等操作；source record通过其source找到behavior operations | service FIB定义泛型`FibEntrySrc<S,...>`和`FibEntrySourceOperations<P,B,S>`函数表；IP插件定义`S`并把concrete operations显式注册进每个family Main，不能用一个trait declaration或封闭`match FibSourceBehavior`冒充registration |
| V29 | `third_party/vpp/src/vnet/fib/fib_entry.h:193-337`; `fib_entry_src.c:67-119` | `fib_entry_src_t`是每个entry的`fe_srcs` value vector元素，按source priority排序；它直接保存path-list、flags、refcount和behavior-private data，不是按behavior分散到多个pool再以index间接引用 | Hammer的`FibEntry.sources`直接保存`FibEntrySrc` records；FES抽象是这些records的注册行为，不是另一组source-record pools |
| V30 | `third_party/vpp/src/vnet/adj/adj.h:216-359`; `adj.c:58-106`; `adj_nbr.c:19-89`; `adj_glean.c:19-94` | adjacency第1 cache line中的subtype union占满剩余48 bytes；第4 cache line先有pointer-sized delegates槽，再有node/lookup/link/nh-protocol/flags；neighbor DB按nh-protocol和interface分区、key为16-byte address+64-bit link，glean DB按protocol/interface分区、key仅为normalized 16-byte prefix address | Hammer即使延期midchain/delegates，也必须显式保留这些layout槽与准确offset；hash分区和key不得按想象改成另一种复合key |
| V31 | `third_party/vpp/src/vnet/adj/adj.c:458-568`; `adj_nbr.c:744-925`; `adj_glean.c:315-430` | generic `adj_walk`只遍历neighbor/mcast，所以MTU callback不遍历glean；neighbor admin-down强制同步walk并临时自锁，glean admin walk也同步；delete只walk已存在对象且interface add不触发resolve | Hammer必须分别实现neighbor与glean event路径，不得写一个“遍历所有adjacency”的统一回调改变VPP可观察语义 |
| V32 | `third_party/vpp/src/vnet/fib/fib_path.c:609-718,1940-1965` | attached path在P2P interface解析为zero-next-hop neighbor adjacency，在NBMA解析为drop，其他interface才解析为glean；attached-next-hop在P2P也使用zero next hop；path仍成为adjacency child并在interface/adjacency down时标记unresolved | backend resolver必须读取interface link facts；connected route不能无条件声称得到glean，尤其tuntap的hw class已经标记P2P |
| V33 | `third_party/vpp/src/vnet/fib/fib_entry_src_adj.c:18-420`; `fib_entry_src.c:1908-1920`; `fib_source.h:250-263`; `ip-neighbor/ip_neighbor.c:330-410` | `FIB_SOURCE_ADJ`以priority `0xd0`选择独立ADJ behavior；模块初始化显式注册ADJ VFT；neighbor host route通过该source增删path；ADJ FES跟踪less-specific cover、维护per-path refinement extension，只在attached cover与相同或unnumbered interface path匹配时安装 | 两个family Main都必须显式注册ADJ operations，IP source state与path extension必须承载cover/sibling和refinement；不能用API/simple behavior代替neighbor host-route语义 |
| V34 | `third_party/vpp/src/vnet/fib/fib_path.c:181,375,957-1203`; `fib_path_list.c:28,451-529,1280-1315`; `fib_entry.h:327-344`; `fib_entry_src.c:1040-1228`; `fib_entry.c:1453-1528` | path保存所属`fp_pl_index`并在收到parent back-walk后直接调用`fib_path_list_back_walk`；winner entry以`fe_parent`/`fe_sibling`成为path-list child；path-list达到64 children后标记popular并异步walk；entry只对源码列出的reasons reactivate，随后把原因归一成`EVALUATE`继续传播 | Hammer必须形成`adjacency -> path -> path-list -> entry`完整依赖链；只有adjacency child/sibling不足以使route forwarding收敛 |
| V35 | `third_party/vpp/src/vnet/fib/fib_entry_src.c:597-650,685-755`; `fib_entry.c:1379-1405` | 每个entry拥有稳定`fe_lb`；首次install创建并插入forwarding trie，后续winner/path/loop变化通过`load_balance_multipath_update`原地修改；只有uninstall才从trie删除并reset | Hammer entry在一次installed生命周期内保持同一LB DPO identity；不得把每次projection建新root并replacement当作对齐语义 |
| V36 | `third_party/vpp/src/vnet/fib/fib_entry_src.c:1247-1341`; `fib_path_list.c:553-575`; `adj_nbr.c:783-797` | FES/path/adjacency操作可能递归分配并使pool relocation，VPP在调用后按稳定index重新取得对象；临时对象指针不得跨越这些调用 | Rust FES function pointer只接收owner、entry index和source identity；callback内部按index分段借用，任何可能扩容的owner operation前必须结束record borrow，返回后重新取得，禁止同时传`&mut FibMain`与其内部`&mut FibEntrySrc` |
| V37 | `third_party/vpp/src/vnet/fib/fib_entry_src_interface.c:296-316`; `fib_entry_src_adj.c:336-418` | INTERFACE VFT只注册init/add/remove/path-swap/activate/deactivate/format/installed/cover-change；ADJ cover-change会deactivate+activate且仅重新可安装时返回`EVALUATE`，cover-update只读取cover的`ATTACHED`并始终返回`bw_reason=NONE` | 每个concrete FES operations table必须逐slot匹配VPP；不能给INTERFACE私增reactivate/cover-update/fwd-update，也不能合并ADJ cover-change与cover-update结果 |
| V38 | `third_party/vpp/src/vnet/interface.h:180-257`; `interface.c:203-225`; `ip4_forward.c:843-876`; `ip6_forward.c:486-518`; `adj_glean.c:326-345`; `adj_nbr.c:801-832` | 普通registration默认为LOW，IP4/IP6与glean admin callbacks均为LOW，neighbor callback显式为HIGH；dispatcher先执行全部LOW再执行HIGH，同级源码没有额外priority合同 | Hammer callback inventory必须保存LOW/HIGH并保证所有LOW先于neighbor HIGH；测试只断言跨priority相对顺序，不把同级安装顺序提升成VPP语义 |
| V39 | `third_party/vpp/src/vnet/adj/adj_glean.c:96-147`; `rewrite.c:85-109,163-171`; `interface.c:1814-1835` | glean创建立即调用interface `update_adjacency`；`vnet_rewrite_init`根据adjacency packet node与interface TX node建立graph edge并写`rewrite.next_index`，之后才由hw class构造rewrite bytes | Hammer即使延期ARP/ND packet producer和L2 rewrite bytes，也必须复用现有DPO/graph stack API得到真实next slot；只写`node_index`或伪造常量next arc不算完成 |
| V40 | `third_party/vpp/src/vnet/fib/fib_path_list.c:34-55,297-329,533-583`; `fib_entry_src.c:1290-1298,1435-1444` | path-list只含embedded node、flags、path indices和uRPF；source ownership与entry child ownership都通过embedded node lock表达，last lock统一销毁path与uRPF | Hammer path-list不得另设`source_count`形成第二套引用事实；source/entry child都使用同一个`FibNode.lock_count`生命周期 |
| V41 | `third_party/vpp/src/vnet/fib/fib_path.c:42-99,1541-1677`; `fib_path_list.c:673-704` | path type有稳定排序值；path-list排序调用通用`fib_path_cmp_for_sort`，先比较preference，再按path type、next-hop protocol和该type字段比较；算法不由IPv4/IPv6 backend各复制一份 | service generic FIB以显式VPP discriminant的`FibPathMode`和通用字段实现一次静态单态化 comparator；next-hop protocol由family Main静态固定，地址差异由`N: Ord`提供，不增加runtime family分支 |
| V42 | `third_party/vpp/src/vnet/fib/fib_walk.c:50-83,640-723,725-805`; `fib_walk.h:24-42` | async walk是pool中的内部FIB node，复制context，以child身份挂到被walk parent从而持锁，同时挂入HIGH/LOW queue；`FORCE_SYNC`拒绝异步并立即sync；完成时解除两个sibling/lock | Hammer async queue不能只保存可能过期的parent index；service必须拥有internal walk record及parent/queue sibling，按同一node-list lifetime完成或取消 |

上述实现路径没有单独的VPP单元测试覆盖全部组合；本文测试矩阵直接由这些源码分支导出，
不把“没有上游测试”误写成“没有语义”。

## 3. 当前 Hammer 基线与阻断项

| ID | Hammer source | 当前事实 | 结论 |
| --- | --- | --- | --- |
| H1 | `crates/hammer-plugins/net/ip/src/interface.rs`, `ip4_add_del_interface_address`, `ip6_add_del_interface_address` | success只更新address pool、enable/link引用并调用callback | 当前实现缺少V1-V7 route side effects，不能作为完成的VPP address operation |
| H2 | `crates/hammer-plugins/net/ip/src/lookup.rs`, `IpLookupMain<A>` | 只有address pool/hash/list，没有prefix pool/hash/refcount | 不能正确共享或最后释放同interface同prefix的glean/special routes |
| H3 | `crates/hammer-plugins/net/ip/src/lookup.rs`, `Ip4Main`/`Ip6Main` | family table存在但只提供worker只读lookup；没有barrier-owned route mutation入口 | route helper必须直接借用owning family table，不增加全局FIB registry或publication wrapper |
| H4 | `crates/hammer-service/src/interface_model.rs`, `set_software_flags` | 只有device-class和hw-class admin callbacks；generic callback inventory只有hw/sw add-del | 必须增加与VPP `sw_interface_admin_up_down_functions`对应的独立registration inventory |
| H5 | `crates/hammer-service/src/net/fib.rs:180-347` | `FibPath`、`FibPathExtList`、`FibEntrySrc`、`FibPathList`和uRPF owner已经存在，字段与ADR-0005目标一致 | 必须直接完成这些现有类型的live graph接线；不得复制为interface-route私有path/source/list类型 |
| H6 | `crates/hammer-service/src/net/fib.rs:385-687`, `FibTable::{add_route,remove_route}` | 当前live入口仍接收已投影`DpoId`，source事实分散在`source_references`与`source_forwarding`两个map；`FibEntry.sources`第二项只是entry index，不是`FibEntrySrc` pool index；entry flags固定为空 | 当前路径没有达到V17-V19或ADR-0005；不能作为interface route的依赖面 |
| H7 | `crates/hammer-plugins/net/ip/src/fib.rs:115-197,296-370` | backend已声明`NextHop`与`PathFlags`associated types，但`project_forwarding`只锁住`entry.forwarding`中的现成DPO；没有解析`FibPath`、选择winner path-list或构造LB/uRPF | associated types证明现有泛型边界已经存在；应补全该边界，不能新增family-erased adapter或由caller投影DPO |
| H8 | `crates/hammer-plugins/net/ip/src/fib.rs:620-705` | 测试单独创建`FibPathList`并手动`bake_urpf`、再手动绑定LB；该path-list从未进入被测`FibTable` | 这里只证明primitive各自可用，不证明live FIB source/path projection已经对齐 |
| H9 | `crates/hammer-service/src/net/dpo.rs`; scoped search under IP plugin | `ReceiveDpo<A>`和DPO class keys存在；旧`AdjacencyDpo<A,R>`把IP adjacency payload放进service且没有VPP layout、lookup-next或subtype union | 删除该泛型payload；local host复用receive owner，connected与`/31`由backend解析到IP插件私有`IpAdjacency` |
| H10 | `crates/hammer-plugins/net/ip/src/interface.rs`, `Ip6LinkMain` | link identity和locks存在，但没有per-interface link-local FIB | link-local add/replace/delete尚未达到V14 |
| H11 | `crates/hammer-service/src/net/fib.rs`; IP adjacency draft | service没有`FibNode`/`FibNodeList`；`FibPathList`内嵌`Vec<FibPath>`且`children`是无owner语义tuple；draft adjacency只有独立lock count，没有path sibling/back-walk，rewrite header字段也不完整 | 这些不是后续优化，而是adjacency/FIB DPO依赖成立的前置条件；必须先补service FIB-node基础设施并把path/adjacency接入 |
| H12 | `crates/hammer-service/src/net/fib.rs`, `FibSourceBehavior`与`FibEntrySrc` | behavior是封闭enum且FIB owner只能静态`match`；没有source allocation/registration或FES operations registration；`FibEntry.sources`不能直接承载按priority排序的source records | 当前设计不能让插件注册source或实现FES behavior，必须先建立V28-V29的service抽象与plugin-owned concrete实现 |
| H13 | `crates/hammer-service/src/interface_model.rs`, `HwClassFlags::P2P`; `crates/hammer-plugins/device/tuntap/src/lib.rs` | service已有P2P class fact且tuntap设置该flag，但没有供IP path resolver读取的窄查询；没有NBMA class fact | 增加只读`InterfaceMain::is_p2p(sw_if_index)`；本ADR不新增不可配置的NBMA flag，非P2P走glean，tuntap必须走zero-next-hop neighbor |

## 4. Owner 与数据模型

### 4.1 Ordinary interface prefix owner

`IpLookupMain`继续由每个family Main按值持有，并增加只服务真实route生命周期的private prefix
state：

```rust
struct IpInterfacePrefix<P, S> {
    prefix: P,
    sw_if_index: u32,
    reference_count: u32,
    source: S,
}

struct IpLookupMain<A, P, S> {
    // ADR-0028 address fields remain.
    interface_prefixes: Pool<IpInterfacePrefix<P, S>>,
    interface_prefix_index_by_key: HashMap<(P, u32), u32>,
    // Existing unicast arc/local-next fields remain.
}
```

`P`分别是`Ipv4Net`和`Ipv6Net`，`S`在IPv4实例中是保存VPP `src_ia_index`的`u32`，在IPv6
实例中是零大小`()`。Rust单态化后IPv6 record没有`Option<u32>`字段、discriminant或运行时分支；
不增加runtime family enum、trait object或`IpFamilyMain`。prefix key使用规范化
network/prefix-length加`sw_if_index`，对应VPP `ip_interface_prefix_key_t`。

prefix record只表达interface-prefix共享生命周期，不复制FIB entry、DPO或address record。
首次引用安装prefix contribution，后续引用只增加`u32`计数；最后引用删除contribution并释放
record。本文不为该VPP refcount路径增加新的public error branch。

### 4.2 FIB抽象与family具体实现边界

`Ip4Main`和`Ip6Main`各自按值拥有一个单态化的
`FibMain<Ipv4Net, Ip4FibBackend, IpFibEntrySource>` /
`FibMain<Ipv6Net, Ip6FibBackend, IpFibEntrySource>`。每个family `FibMain`拥有table集合，以及该family全部table
共享的entry、source、path-list和path pools；具体`FibTable<P,B>`只保存一张table的prefix/backend
状态。这样`FibNodePtr(type,index)`中的index在对应family/type pool内唯一，不需要裸指针、table-id
拼装handle或每table动态node type。为了main-thread Worker Barrier内修改，family Main提供private
direct mutable borrow；不把table移到
`InterfaceMain`、`NetMain`或runtime registry，也不新增`Arc`、lock、atomic pointer或clone-
publish。worker lookup仍在barrier之外只读已发布table。

route helper必须先通过family `fib_index_by_sw_if_index`取得真实`fib_index`，再索引family
table。`sw_if_index`永远不直接当作`fib_index`。

边界按现有泛型静态组合固定如下：

| Layer | Owns | Must not own |
| --- | --- | --- |
| `hammer-service::net::fib_node` / `fib_node_list` | `FibNode`、`FibNodeType`、`FibNodePtr`、`FibNodeList`、`FibNodeSibling`、`FibNodeOperations`与registration；heterogeneous child-list pools、lock/child bookkeeping及back-walk dispatch | plugin object pool、IP adjacency/path subtype、DPO或family state；registered function只按`(type,index)`回到concrete owner |
| `hammer-service::net::fib` | 泛型`FibMain<P,B,S>`、`FibTable<P,B>`、`FibEntry`、`FibEntrySrc<S,...>` record、可注册`FibEntrySourceOperations<P,B,S>`、node-backed `FibPath<N,F>`与`FibPathList<N,F>`；source registry、winner、完整back-walk链、activation/reactivation和entry-owned稳定LB生命周期 | concrete source state `S`、`Ipv4Addr`、`Ipv6Addr`、`Ipv4Net`、`Ipv6Net`、`IpPathFlags`、IP mask、receive/glean/neighbor选择、family forwarding trie或interface-address policy |
| `hammer-plugins/net/ip::adjacency` | 单一`Pool<IpAdjacency>`、IP prefix/subtype/rewrite layout、glean/neighbor DB、真实family node index、adjacency child/last-lock/MTU/back-walk静态dispatch | generic node-list storage、generic DPO registry、ARP/ND policy下沉service或per-family duplicate object pool |
| `hammer-plugins/net/ip::fib::Ip4FibBackend` | `Ipv4Net`prefix store、IPv4 next-hop/`IpPathFlags`解释、IPv4 path fixup和receive/glean/attached-neighbor解析、IPv4 LB/uRPF projection、IPv4 forwarding trie | generic source排序、重复source引用、path-list通用生命周期或另一套entry graph |
| `hammer-plugins/net/ip::fib::Ip6FibBackend` | `Ipv6Net`prefix store、IPv6 next-hop/`IpPathFlags`解释、IPv6 path fixup和receive/glean/attached-neighbor解析、IPv6 LB/uRPF projection、IPv6 forwarding trie | generic source排序、重复source引用、path-list通用生命周期或另一套entry graph |
| IP interface route helpers | 从具体IPv4/IPv6 address事实构造对应family的prefix、entry flags与`FibPath`，选择FIB operation | source winner、path-list、DPO/LB/uRPF construction、forwarding trie mutation或跨family dispatch |

`Ip4FibMain = FibMain<Ipv4Net, Ip4FibBackend, IpFibEntrySource>`与
`Ip6FibMain = FibMain<Ipv6Net, Ip6FibBackend, IpFibEntrySource>`通过Rust单态化共享抽象逻辑；不增加
`dyn FibBackend`、runtime family enum、`IpFamilyMain`或把两个backend合成一个ip46实现。

### 4.3 FIB prerequisite gate

interface routes不得先接到当前`FibTable::add_route(prefix, source, DpoId)`上。该入口已经有可复用
的backend publication、cover forwarding恢复和DPO root释放代码，但它绕过了仓库中已经存在的
`FibEntrySrc`、`FibPath`与`FibPathList`，不是VPP的source/path graph。先“临时”把interface route
投影成LB会形成第二条事实链，之后无法正确实现winner切换、cover/glean source迁移或uRPF重建。

本文把ADR-0005已经批准、但尚未接入live table的FIB设计设为第一阶段硬门槛。不是新建一组
interface-route类型，而是完成现有类型：

```text
FibMain<P, B, S>
  -> FibTable<P, B>
  -> family-global FibEntry pool
     -> priority-sorted Vec<FibEntrySrc<S, ...>>
        -> registered FibSource -> registered FibEntrySourceOperations<P, B, S>
           -> family-global FibPathList<B::NextHop, B::PathFlags> pool
              -> stable indices in family-global FibPath<B::NextHop, B::PathFlags> pool
                 -> adjacency child/sibling
              -> entry parent/sibling child
  -> winner source activation/reactivation
  -> update the entry-owned stable LoadBalanceDpo in place
     from winner flags/path-list/cover facts + baked FibUrpfList
  -> B::forwarding_update
  -> family forwarding trie
```

第一阶段必须同时满足：

1. `FibEntry.sources`直接保存按registered source priority排序的`FibEntrySrc` value records，删除
   `source_references`、`source_forwarding`及按behavior拆分的source-record pools；record自己持有
   `ref_count`、`entry_flags`、source flags、path-list link和behavior-private data。
2. source按现有`FibSource.priority`排序，priority更小者胜出；增加loser只保存其source/path事实，
   不改变packet forwarding；删除winner会activate下一source并重投影；删除最后source才移除entry。
3. VPP操作语义保持分离：`update`/`update_one_path`替换一个source的完整path set；
   `path_add/path_remove`增量修改；`special_add/special_remove`才使用重复add的source refcount；
   owner-internal special DPO仍是特殊入口。不得把这些操作重新压成一个含糊的`add_route`。
4. ordinary path输入保留为现有`FibPath<B::NextHop, B::PathFlags>`；table执行path fixup、稳定排序，
   path-list按完整paths与key flags共享。共享list采用copy-on-write修改，source/path-list/DPO引用各自
   计数，不相互冒充。
5. winner activate时entry连接winner的path-list；winner update时reactivate；winner change时先
   deactivate旧source再activate新source。`FibEntry.flags`来自当前winner的
   `FibEntrySrc.entry_flags`，不是entry创建时固定为空，也不是所有source flags的并集。
6. 已注册的interface FES behavior完成local source对less-specific cover的通用跟踪：cover/sibling
   保存在interface source record，`PROVIDES_GLEAN`表达哪个local source当前为cover提供事实；
   它不读取IP地址或修改adjacency。IPv4/IPv6 backend根据这些通用source/cover facts更新各自
   concrete glean source address；删除或winner/cover变化时从仍有效local source中重新选择。
7. IPv4/IPv6 backend分别从winner path-list逐path解析receive、glean、attached-neighbor或其他concrete child；
   generic FIB为entry首次install创建一个LB，后续projection通过LB owner原地更新buckets/flags/uRPF。
   同一installed生命周期不得替换entry LB identity；只有uninstall才从trie删除并释放该LB。
8. `FibPathList`从path语义烘焙uRPF并由entry projection绑定到LB。attached与attached-next-hop即使
   neighbor尚未resolve也贡献`sw_if_index`；receive path不因DPO中带interface就自动贡献。
   `LOOSE_URPF_EXEMPT`按VPP作为entry-local projection规则处理，不能修改共享path-list。
9. DPO引用按VPP source/path-list生命周期取得和释放：path replacement释放旧path DPO，LB owner在原地
   bucket update中维护bucket/uRPF引用；不得为每次projection创建replacement root，不得由interface helper
   持有construction root，也不得增加事务层或把DPO生命周期改造成route mutation error。
10. 所有mutation继续要求main thread持有现有Worker Barrier；不增加FIB lock、snapshot、
    `FibTableHandle`、family-erased trait object或第二套publication协议。
11. `FibPath`和`FibPathList`必须嵌入`FibNode`并使用稳定pool index；path解析到receive/glean/
    neighbor child时取得DPO引用，并对adjacency调用child-add保存`FibNodeSibling`；re-resolve和销毁
    先child-remove再释放旧DPO。path还必须保存所属path-list index；winner entry保存parent path-list与
    sibling并成为其child。`Vec<FibPath>` value copy不能作为live path identity。
12. `FibNodeOperations`与`FibEntrySourceOperations<P,B,S>`使用注册时生成的静态函数指针表，不使用`dyn`、
    `Any`、闭包或type-erased plugin state。service只保存操作函数和name/id；concrete pool与state仍由
    注册插件拥有。node/FES dispatch发生在control-plane graph mutation/back-walk，不进入packet hot
    path。depth超过32是graph loop的owner invariant，不返回route error。
13. back-walk固定经过`adjacency -> path -> path-list -> entry`：path按reason更新resolved/DPO状态后以保存的
    path-list index继续；path-list重建uRPF，child count首次达到64时永久标记`POPULAR`，popular list异步
    walk，其余同步，`FORCE_SYNC`使admin-down至少
    同步到首个entry；entry只对`EVALUATE`、`ADJ_UPDATE`、`ADJ_DOWN`及interface up/down/bind/delete
    reactivate稳定LB，`ADJ_MTU`不触发entry reactivate；所有reason向entry children继续传播前都归一成
    `EVALUATE`并清除`FORCE_SYNC`。path-list不是path的FIB child，不伪造二者的sibling关系。

这十三项及第11.1节FIB测试全部通过，才算FIB prerequisite gate关闭。gate关闭前不得实现或接线
`ip4_add_del_interface_routes`、`ip6_add_del_interface_routes`，也不得用当前直接DPO入口保存临时
interface routes。

### 4.4 Interface route作为FIB consumer

第二阶段的family helpers只构造entry flags和现有`FibPath`输入，并调用第一阶段完成的FIB owner
operation：

| Interface contribution | FIB operation | Existing path facts |
| --- | --- | --- |
| IPv4/IPv6 local host | `update_one_path` | host next-hop、真实`sw_if_index`、weight 1；fixup得到local/receive path |
| connected prefix | `update_one_path` | zero next-hop、真实`sw_if_index`、weight 1；connected+attached flags使fixup得到attached path，resolver再按interface link选择zero-neighbor或glean |
| IPv4 `/31` peer host | `update_one_path` | peer next-hop、真实`sw_if_index`、weight 1；attached-next-hop在P2P使用zero-neighbor，普通interface使用peer neighbor |
| IPv4 network/base drop | `special_add` / `special_remove` | drop + loose-uRPF-exempt entry flags；没有伪造path或caller-built LB |
| IPv4 broadcast drop | `special_add` / `special_remove` | directed-broadcast-disabled下同上 |

family helper不创建receive/adjacency/LB/uRPF，不调用`create_load_balance`，不写
`source_forwarding`，不直接写`Ip4ForwardingTrie`/`Ip6ForwardingTrie`。这些全部由FIB winner、
family具体path resolver和backend projection统一完成。抽象FIB只编排winner与projection合同，
不解释IP path。`FibSource::INTERFACE`在`FibSourceMain`中注册为priority `0x03`并指向已注册的
interface FES behavior；helper不能绕过source registry或behavior dispatch。

### 4.5 DPO projection

interface-source route通过第一阶段完成的ADR-0005既有FIB source/path投影形成
`LoadBalanceDpo`，而不是直接
写`Ip4ForwardingTrie`/`Ip6ForwardingTrie`：

| Contribution | Required forwarding child |
| --- | --- |
| IPv4 local host | concrete `ReceiveDpo<Ipv4Addr>`，保存IPv4 address与真实`sw_if_index` |
| IPv6 local host | concrete `ReceiveDpo<Ipv6Addr>`，保存IPv6 address与真实`sw_if_index` |
| connected prefix on P2P（包括tuntap） | IP插件同一个`IpAdjacency` pool中的zero-next-hop incomplete-neighbor投影 |
| connected prefix on ordinary interface | glean投影，subtype保存`IpFibPrefix`，rewrite保存egress interface/MTU |
| IPv4 `/31` peer host | incomplete-neighbor投影；P2P key使用zero next hop，ordinary interface使用另一个endpoint |
| IPv4 network/base host | protocol drop DPO |
| IPv4 broadcast host（默认directed broadcast关闭） | protocol drop DPO |

backend通过新增的只读`InterfaceMain::is_p2p(sw_if_index)`读取现有`HwClassFlags::P2P`，不缓存或
复制class事实。Hammer尚无NBMA class flag或可配置owner，因此本ADR没有可到达的NBMA branch，
不为它新增虚假state；以后引入NBMA interface时，attached resolver必须按V32增加drop分支。

每个FIB forwarding root最终是load-balance identity；FIB拥有该root引用，load-balance拥有child
引用，adjacency/receive owner在最后引用释放时回收对象。route replacement、withdrawal和table
drop沿既有`lock_dpo`/`unlock_dpo`链释放，不能保存裸pool pointer或从`Copy DpoId`推断引用。

`InterfaceRxDpo`不在本表中。它表示把packet RX identity改成指定interface并重新进入IP input，
与VPP interface-source route的receive/glean/attached adjacency完全不同。

### 4.6 FIB node/list 与 IP adjacency owner/layout

service net新增`fib_node`和`fib_node_list`模块。它们拥有所有FIB对象共用的graph事实、全局
list head/element pools和node-type operations registry，但不拥有任何plugin对象pool或plugin
Main引用。插件通过service定义的抽象注册自己的node type实现：

```rust
#[repr(transparent)]
pub struct FibNodeType(u8);

#[repr(C)]
pub struct FibNodePtr {
    pub node_type: FibNodeType,
    pub index: u32,
}

#[repr(transparent)]
pub struct FibNodeList(u32);

#[repr(transparent)]
pub struct FibNodeSibling(u32);

#[repr(C)]
pub struct FibNode {
    node_type: FibNodeType,
    owner_data: u16,
    children: FibNodeList,
    lock_count: u32,
}

pub struct FibNodeOperations {
    // Required owner operations corresponding to VPP fnv_get/fnv_last_lock.
    // Optional back_walk/memory operations remain explicit Option<fn> slots.
    get: FibNodeGet,
    last_lock: FibNodeLastLock,
    back_walk: Option<FibNodeBackWalk>,
    memory: Option<FibNodeMemory>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FibWalkPriority {
    High,
    Low,
}

impl FibNodeOperations {
    pub const fn new(get: FibNodeGet, last_lock: FibNodeLastLock) -> Self;
    pub const fn with_back_walk(self, back_walk: FibNodeBackWalk) -> Self;
    pub const fn with_memory(self, memory: FibNodeMemory) -> Self;
}
```

`FibNode`必须保持12 bytes，`owner_data`/`children`/`lock_count` offset分别为2/4/8；
`FibNodePtr`保持8 bytes且`index` offset为4，只保存type/index，不保存会因pool扩容失效的Rust引用
或裸指针。`FibNodeList`、`FibNodeSibling`和node index均以`u32::MAX`为invalid sentinel。
`FibNodeListMain`
内部使用`Pool<FibNodeListHead>`和`Pool<FibNodeListElement>`，element保存所属list、owner pointer、
next和previous，返回的`sibling`就是element index。

`FibNodeOperations`是service定义的同构操作表，不含plugin state。插件为自己的concrete pool定义
普通静态函数并注册；`get`返回的owner borrow只允许在一次service graph operation内部使用，不能
公开、缓存或跨越可能扩容pool的调用。注册时必须有`get`和`last_lock`，一个type只能注册一次；
`back_walk`缺失只允许从不作为child的node type。该注册路径对应VPP
`fib_node_register_type/new_type`，不能退化为IP插件里的封闭`match FibNodeType`。

service暴露的窄操作为：

```rust
impl FibNodeMain {
    pub fn register_type(
        &mut self,
        name: &'static str,
        operations: FibNodeOperations,
    ) -> FibNodeType;
    pub fn child_add(&mut self, parent: FibNodePtr, child: FibNodePtr) -> FibNodeSibling;
    pub fn child_remove(&mut self, parent: FibNodePtr, sibling: FibNodeSibling);
    pub fn child_count(&self, parent: FibNodePtr) -> u32;
    pub fn walk_sync(&mut self, parent: FibNodePtr, context: &mut FibNodeBackWalkContext);
    pub fn walk_async(
        &mut self,
        parent: FibNodePtr,
        priority: FibWalkPriority,
        context: FibNodeBackWalkContext,
    );
    pub fn run_queued_walks(&mut self, budget: usize) -> usize;
}

impl FibNode {
    pub fn new(node_type: FibNodeType) -> Self;
    pub fn lock(&mut self);
    pub fn unlock(&mut self) -> bool;
}
```

`child_add`经registered `get`定位parent，先增加parent lock并惰性创建list；`child_remove`移除
sibling、在空list时释放head，再释放parent lock；若成为last lock则经registered operation通知
concrete owner回收。underflow、错list sibling、重复type注册和带children deinit是owner invariant
assertion，不定义新error。list内部walk先保存next sibling再dispatch child，允许child在back-walk中
解除自己；公开读取使用直接iterator，不接受closure。

`walk_async`分配service-private `FibWalk` record，复制context，把walk node作为child挂到parent并保存
parent sibling，从而在排队期间锁住parent；同一walk再以queue sibling挂到HIGH/LOW node-list。
`run_queued_walks`由既有control-plane main-loop owner按HIGH再LOW及budget推进，逐项仍调用相同child-list
walker；完成或取消时先解除queue sibling，再解除parent sibling并释放walk slot。queue和record只保存
type/index/context/siblings，不保存owner borrow。`FORCE_SYNC`拒绝入队并立即调用`walk_sync`，且在首个entry
清除，不能复制进后续异步递归；shutdown必须在销毁node pools前排空或取消全部walk records。本文的
interface admin-down入口走同步链，popular path-list只有在未被`FORCE_SYNC`约束时才排LOW async walk。

`FibNodeBackWalkContext`保存reason flags、walk flags、depth和interface-bind table indexes；最大depth
为32。service不做downcast、不保存plugin Main引用或object指针；registry只保存复制的静态operation
functions与type name。每个plugin把返回的type保存在自己的Main，concrete operation从自己的typed
pool按index取对象。这里允许的是VPP FIB graph要求的注册式control-plane dispatch，不是生产代码中的
`dyn` trait object，也不是把plugin state或引用装入generic service registry。

entry、path-list和path也必须注册各自的concrete operations，而不是只注册adjacency。path back-walk
operation按V34处理reason并用`path_list_index`调用path-list owner operation；path-list owner先重建uRPF，
再按`POPULAR`和`FORCE_SYNC`选择同步或异步walk其entry children；entry operation reactivate当前winner
并原地更新稳定LB，随后把继续传播的reason改为`EVALUATE`。任何operation在调用可能扩容pool的子操作前
保存自身index并结束当前borrow，调用后重新按index借用；registered function不得返回或缓存owner borrow。

`IpFibPrefix`和`IpAdjacency`都是IP插件私有具体类型。`hammer-service`除此之外只保留`DpoId`、
DPO class注册/stack/引用操作以及泛型FIB graph，不定义、导出或保存IP adjacency payload。

`IpFibPrefix`对应VPP `fib_prefix_t`，不是`IpNet`的别名或wrapper：

```rust
#[repr(u8)]
enum IpFibProtocol {
    Ip4,
    Ip6,
}

#[repr(C)]
struct IpFibPrefix {
    length: u16,
    protocol: IpFibProtocol,
    padding: u8,
    address: Ip46Address, // fixed 16-byte IP46 storage
}
```

必须以编译期断言固定`size_of::<IpFibProtocol>() == 1`、`size_of::<IpFibPrefix>() == 20`且
`address` offset为4。IPv4地址按
VPP `ip46_address_t` 放在最后4字节，前12字节清零；IPv6使用全部16字节。`normalize`、cover
和数据库key在该类型上完成；`Ipv4Net`/`Ipv6Net`只出现在family backend/API转换边界。

`IpAdjacency`对应当前slice用到的`ip_adjacency_t`布局：

```rust
#[repr(C)]
union IpAdjacencySubtype {
    neighbor: Ip46Address,
    glean: IpFibPrefix,
    reserved: [u8; 48], // preserves VPP's complete first-cache-line union slot
}

#[repr(u8)]
enum IpAdjacencyLink {
    Ip4,
    Ip6,
    // Remaining VPP link values are reserved but not constructed by this ADR.
}

#[repr(C)]
struct IpAdjacency {
    cacheline0: CacheLineAlignMark,
    node: FibNode,
    config_index: u32,
    subtype: IpAdjacencySubtype,

    cacheline1: CacheLineAlignMark,
    rewrite: IpAdjacencyRewrite, // exactly two cache lines

    cacheline3: CacheLineAlignMark,
    delegates: usize, // pointer-sized VPP layout slot; always zero in this ADR
    node_index: u32,
    lookup_next: IpLookupNext,
    link: IpAdjacencyLink,
    next_hop_protocol: IpFibProtocol,
    flags: u8,
    padding: [u8; 48],
}
```

`IpAdjacencyRewrite`不是opaque 128-byte padding，必须精确包含`sw_if_index: u32`、
`next_index: u16`、`data_bytes: u16`、`max_l3_packet_bytes: u16`、flags、
`dst_mcast_offset: u8`和116-byte rewrite data，总长128 bytes。实现只支持本仓库现有64-bit targets，
必须断言64-byte alignment及`size_of::<usize>() == 8`。
`IpAdjacency`总长256 bytes、embedded `FibNode` offset 0、`config_index` offset 12、subtype
offset 16/size 48、`rewrite` offset 64、第四cache line offset 192、`delegates` offset 192、
`node_index` offset 200、四个1-byte控制字段offset 204..207以及padding offset 208。
`IpLookupNext`保留VPP值：incomplete/ARP为3、glean为4。`IpFibProtocol`和`IpAdjacencyLink`
均固定为1 byte并分别对应VPP `fib_protocol_t`和`vnet_link_t`；二者都不是`DpoProto`，只有构造DPO identity
时执行显式转换。当前slice只允许按
`lookup_next`读取union中对应的neighbor或glean member；不为未实现的rewrite/midchain/mcast
状态伪造Rust enum variant。

所有IPv4/IPv6 neighbor与glean对象来自一个`IpAdjacencyMain.adjacencies` pool。DB严格使用VPP的
两级分区：glean先按`(protocol, sw_if_index)`选表，再以normalized `Ip46Address`为key，prefix length
只保存在对象的`IpFibPrefix`中；neighbor先按`(next_hop_protocol, sw_if_index)`选表，再以
`(Ip46Address, IpAdjacencyLink-as-u64)`为key。DPO identity先以邻接class建立，再按对象
`lookup_next`投影为`ADJACENCY_GLEAN`或`ADJACENCY_INCOMPLETE`，protocol来自link到DPO protocol
的显式映射。DPO lock/unlock必须调用embedded `FibNode` lock/unlock；last lock先断言无children，
再按subtype删除DB、deinit node并在barrier内释放pool slot。MTU和uRPF读取同一对象。Graph Node仍按VPP
拆为`ip4-glean`、`ip6-glean`、`ip4-adjacency-incomplete`和
`ip6-adjacency-incomplete`，不创建统一family node。

glean首次创建时subtype保存normalized connected prefix；interface FES behavior选出同网段
local provider后，只替换`IpFibPrefix.address`而保留connected prefix length。last unlock对该
prefix normalize即可恢复hash key。这对应`fib_entry_src_interface_update_glean`，不能在旁边
增加独立`source: Option<IpAddr>`字段形成第二份事实。

glean创建时把该family的真实glean Graph Node ID写入`node_index`；incomplete neighbor同样保存
对应ARP/neighbor-discovery node，而不是`u32::MAX`。两种创建路径都初始化config index、flags、
rewrite interface/data length 0/MTU，并保持reserved subtype/delegate/padding为确定值；不能只靠zeroed
256-byte blob碰巧满足layout。`rewrite.next_index`不能伪造：创建路径复用现有
`DpoMain::stack_from_node(runtime, adjacency_node, DpoId::interface_tx(proto, sw_if_index))`，建立从该family
glean/incomplete node到interface TX node的真实graph edge，并把返回stacked identity中的slot写入
`next_index`。所需DPO class、packet node和interface TX node必须在route mutation前完成初始化；缺失是
plugin/graph初始化合同错误，不转换成route errno。adjacency创建在已持有barrier、node IDs与interface TX
DPO均已验证后调用checked stack API；此后仍返回`Err`表示owner违反初始化/graph invariant，以包含
node/interface identity的local assertion处理，不把`DpoError`加入FIB或address API，`add_or_lock`对调用者仍保持VPP
pool-index返回形状。当前`HwClass::build_rewrite/update_adjacency`仍是
`Option<fn()>`占位，本ADR不谎称已执行VPP的device rewrite bytes callback；把它类型化并生成实际L2
rewrite bytes属于packet-capable adjacency后续ADR。path resolver在取得adjacency DPO后把稳定
path node index注册为child并保存sibling。interface MTU变化使用VPP generic adjacency walk，只遍历
neighbor（本slice无mcast），更新rewrite MTU后以`ADJ_MTU`原因walk children；不把glean私自加入该
walk。ordinary address/admin route withdrawal通过path teardown解除child，不允许带children直接
回收adjacency。

本次对adjacency的完整差异审查如下，避免靠后续逐项补洞：

| Area | 本ADR必须完成 | 明确延期 |
| --- | --- | --- |
| object/layout | 单一256-byte、64-byte aligned `IpAdjacency` pool；embedded 12-byte `FibNode`；48-byte subtype union；完整128-byte rewrite；第4 line保留pointer-sized delegates槽、node/四个1-byte字段和48-byte pad；逐项offset断言 | midchain槽的next-DPO/fixup实际语义、mcast subtype和delegate元素生命周期 |
| identity/database | glean按protocol/interface分表且key仅为normalized 16-byte address；neighbor按nh-protocol/interface分表且key为address+64-bit link；对象保存完整prefix/link；lookup-next变化只改变同一pool index的DPO class投影 | MPLS/ethernet link adjacency、broadcast/mcast DB |
| allocation/init | 只在main thread/barrier内扩容pool；初始化embedded node、nh protocol、flags/config、invalid interface、lookup-next、null delegates及reserved bytes；返回stable pool index | adjacency counters和debug poison |
| graph ownership | service `FibNodeMain`/list pools与registered `FibNodeOperations`；IP插件注册adjacency以及两个family的entry/path-list/path node types并拥有typed pools；path保存所属path-list并作为adjacency child，entry作为winner path-list child；terminal adjacency收到作为child的back-walk是owner invariant；last-lock要求child list为空 | `dyn FibNode`、plugin object/reference下沉service |
| DPO operations | adjacency、incomplete、glean三个class使用同一pool index；incomplete/glean分别注册IP4/IP6 nodes；共享lock/unlock、MTU、uRPF、format和memory owner | complete rewrite/midchain/mcast forwarding行为 |
| rewrite publication | glean/incomplete创建写真实node index、interface、MTU和data length 0；复用`DpoMain::stack_from_node`连接对应packet node到interface TX DPO并保存真实next slot；neighbor MTU callback在barrier内更新并以`ADJ_MTU` back-walk；packet能力不宣称完成 | 类型化并调用interface build/update-adjacency hook、实际device rewrite bytes、neighbor MAC完成/失效导致的ARP/REWRITE双walk、output-feature/MAC-address/table-bind rewrite rebuild |
| glean source | 创建时存normalized connected `IpFibPrefix`；interface source provider变化通过独立owner operation只替换address；删除时normalize恢复DB key | `adj_glean_get_src`供完整ARP/ND发包路径选择source |
| packet nodes | graph中保留四个按family/class拆分的节点和正确DPO stacking；本ADR测试只断言route解析到这些DPO/node identities | `ip4-glean`生成ARP、`ip6-glean`/incomplete生成ND以及neighbor event subsystem；在这些owner存在前节点不得伪装成已实现转发 |
| interface events | MTU只按VPP generic walk遍历neighbor并触发`ADJ_MTU`；neighbor admin-down强制sync且walk期间临时自锁，glean admin walk同步；delete分别walk glean/neighbor，interface add不触发resolve | hardware-link callbacks、output-feature、MAC-address和table-bind rewrite rebuild |
| lifecycle | main-thread barrier mutation；node lock即DPO lock；DB removal、node deinit、delegate槽断言为空、pool release按last-lock顺序；walk先收集indices以容忍回调删除；interface route teardown解除全部path children | BFD up/down、delegate notifications、per-adjacency packet/byte counters |

当前`adjacency.rs` draft不得原样进入实现commit；已知替换清单如下：

| Current draft | Why it is wrong | Required replacement |
| --- | --- | --- |
| 独立`lock_count: u32` | 绕过FIB graph，DPO lock与child lock不是同一生命周期 | offset 0嵌入`FibNode`，所有lock/last-lock经registered node owner |
| subtype union只含16/20-byte active member | 总size靠隐式padding碰巧达到，不能保证VPP 48-byte union槽 | 显式48-byte reserved member与offset/size断言 |
| rewrite使用`u8 data_bytes`、无`next_index`/mcast offset、120-byte data | 字段宽度和header layout错误 | VPP 12-byte header + 116-byte data，完整128 bytes |
| 第4 cache line从`node_index`开始 | 漏掉8-byte delegates槽，后续字段全部偏移错误 | delegates offset 192，node offset 200，control bytes 204..207 |
| FIB protocol、link和DPO protocol都用`DpoProto` | 三个domain概念被混成一个值，neighbor key也丢link | `IpFibProtocol`、`IpAdjacencyLink`和显式DPO转换 |
| glean key为`(IpFibPrefix, sw_if_index)` | 私自把prefix length放进VPP只按address查找的key | protocol/interface分区 + normalized `Ip46Address` key |
| neighbor key为`(Ip46Address, sw_if_index)` | 丢失link，无法表示VPP同next-hop不同link identity | nh-protocol/interface分区 + `(address, link-as-u64)` key |
| glean另存`source: Option<IpAddr>`或从`IpNet` value重建 | 与subtype prefix形成第二份source事实 | 只更新`IpFibPrefix.address`，删除时normalize同一对象恢复key |
| `node_index = u32::MAX` | DPO不能stack到真实family graph node | 创建时写入具体glean或neighbor-discovery Node ID |
| `ip[46]-glean`/incomplete node无条件silent drop | 不等于VPP ARP/ND处理，也没有VPP node-error语义 | 本ADR只验control-plane identity；packet nodes不得计为完成，后续neighbor ADR实现后才能开放packet能力 |
| DPO unlock直接从pool删除 | 不检查children、不deinit node、不按subtype恢复完整DB key | registered last-lock -> assert no children -> DB remove -> node deinit -> pool release |
| path resolver无P2P分支 | tuntap已标记P2P，却会错误创建glean/peer-specific neighbor | 读取`is_p2p`，connected与attached-next-hop都使用zero-next-hop neighbor |

延期项不得以空callback、始终drop且无VPP error语义的“完成节点”、伪造rewrite或service-level
占位类型实现。本ADR的完成声明仅覆盖FIB/route可观察identity和control-plane lifecycle，不宣称
ARP/ND packet resolution已实现。

### 4.7 FIB source 与 FES registration

`FibSource`不是携带priority/behavior副本的开放struct，也不是列出所有插件source的封闭enum；它是
service定义的1-byte source identity。service net的`FibSourceMain`保存注册事实：owned source name、唯一
priority和`FibEntrySourceBehaviorId`。behavior id提供VPP现有drop/API/simple/RR/MPLS/interface/
interpose/LISP/adjacency概念的固定常量；本文不发明dynamic behavior allocation。内建source以固定
identity注册；插件source通过allocate取得identity并选择一个已定义behavior。priority class相同仍
分配唯一slot，winner比较读取registry，不相信caller重复携带的字段。source空间耗尽返回
`FibSource::INVALID`，与VPP一致，不新增allocation `Result`。

FES registration分成两个真实registry，不能用“实现了一个trait”代替注册：

1. process-global `FibSourceMain`注册`FibSource -> (name, priority class/slot, behavior id)`；
2. 每个concrete `FibMain<P,B,S>`注册
   `FibEntrySourceBehaviorId -> FibEntrySourceOperations<P,B,S>`。

route mutation的dispatch固定为：

```text
FibEntrySrc.source
  -> FibSourceMain::registration(source).behavior
  -> FibMain<P,B,S>::entry_source_operations[behavior]
  -> registered static operation
```

service公开的是operations值与显式registration API：

```rust
#[repr(transparent)]
pub struct FibEntrySourceBehaviorId(u8);

pub struct FibEntrySourceOperations<P, B, S>
where
    B: FibTableBackend<Prefix = P>,
{
    pub init: Option<FibEntrySourceInit<P, B, S>>,
    pub deinit: Option<FibEntrySourceDeinit<P, B, S>>,
    pub activate: Option<FibEntrySourceActivate<P, B, S>>,
    pub deactivate: Option<FibEntrySourceDeactivate<P, B, S>>,
    pub reactivate: Option<FibEntrySourceReactivate<P, B, S>>,
    pub add: Option<FibEntrySourceAdd<P, B, S>>,
    pub remove: Option<FibEntrySourceRemove<P, B, S>>,
    pub path_swap: Option<FibEntrySourcePathSwap<P, B, S>>,
    pub path_add: Option<FibEntrySourcePathAdd<P, B, S>>,
    pub path_remove: Option<FibEntrySourcePathRemove<P, B, S>>,
    pub cover_change: Option<FibEntrySourceCoverChange<P, B, S>>,
    pub cover_update: Option<FibEntrySourceCoverUpdate<P, B, S>>,
    pub format: Option<FibEntrySourceFormat<P, B, S>>,
    pub installed: Option<FibEntrySourceInstalled<P, B, S>>,
    pub forwarding_update: Option<FibEntrySourceForwardingUpdate<P, B, S>>,
    pub source_data: Option<FibEntrySourceGetData<P, B, S>>,
    pub source_data_mut: Option<FibEntrySourceSetData<P, B, S>>,
    pub contribute_interpose: Option<FibEntrySourceContributeInterpose<P, B, S>>,
    pub flags_change: Option<FibEntrySourceFlagsChange<P, B, S>>,
    pub copy: Option<FibEntrySourceCopy<P, B, S>>,
}

impl<P, B, S> FibEntrySourceOperations<P, B, S>
where
    B: FibTableBackend<Prefix = P>,
{
    pub const fn new() -> Self;
}

impl<P, B, S> FibMain<P, B, S>
where
    B: FibTableBackend<Prefix = P>,
    S: Default + Clone,
{
    pub fn register_entry_source_behavior(
        &mut self,
        behavior: FibEntrySourceBehaviorId,
        operations: FibEntrySourceOperations<P, B, S>,
    );
}
```

`FibEntrySrc<S,N,F,PathExt>`中的`S`就是注册该family FIB的插件定义的source-specific state；service
不定义这个state的variant。`FibMain<P,B,S>`、`FibEntry<P,N,F,S,PathExt>`和全部FES function alias
沿同一个`S`单态化。IP插件定义`IpFibEntrySource`，其stateless variant供simple/API使用，interface
和adjacency variants分别保存各自的cover entry index与`FibNodeSibling`；service既不知道这些
variants，也不提供backend-wide
opaque bytes、downcast或按behavior分开的record pool。

`FibEntrySourceOperations`字段是service定义的普通`fn`类型别名；每个alias的共同前缀固定为
`fn(fib: &mut FibMain<P,B,S>, entry_index: u32, source: FibSource, ...)`，后面只追加该action需要的
path/DPO/flags参数，不直接接收`&mut FibEntry`或`&mut FibEntrySrc`，也不携带environment。registry保存的是
复制后的static function pointers，不保存`dyn`、`Any`、closure、plugin Main引用或source object
引用。callback通过`FibMain::entry_source(entry_index, source)`和
`FibMain::entry_source_mut(entry_index, source)`取得直接借用；借用只活到当前局部操作结束，调用任何可能
扩容entry/path-list/path pool的owner方法前必须结束该borrow，调用后重新按index取得。这里不定义public
`FibEntrySourceBehavior` trait；operations value就是registration contract。`new()`生成全`None`表，
plugin以public typed slots构造concrete value；只有`register_entry_source_behavior(behavior, operations)`
执行后，该behavior才已注册。

IP family aliases把source definition作为显式类型参数固定下来，而不是藏在backend associated type中：

```rust
type Ip4FibMain = FibMain<Ipv4Net, Ip4FibBackend, IpFibEntrySource>;
type Ip6FibMain = FibMain<Ipv6Net, Ip6FibBackend, IpFibEntrySource>;
```

IP plugin初始化时必须真实执行以下注册；不是测试专用，也不是只声明类型：

```rust
fn register_ip4_fib_entry_sources(fib: &mut Ip4FibMain) {
    fib.register_entry_source_behavior(
        FibEntrySourceBehaviorId::SIMPLE,
        ip4_simple_entry_source_operations(),
    );
    fib.register_entry_source_behavior(
        FibEntrySourceBehaviorId::API,
        ip4_api_entry_source_operations(),
    );
    fib.register_entry_source_behavior(
        FibEntrySourceBehaviorId::INTERFACE,
        ip4_interface_entry_source_operations(),
    );
    fib.register_entry_source_behavior(
        FibEntrySourceBehaviorId::ADJACENCY,
        ip4_adjacency_entry_source_operations(),
    );
}

fn register_ip6_fib_entry_sources(fib: &mut Ip6FibMain) {
    fib.register_entry_source_behavior(
        FibEntrySourceBehaviorId::SIMPLE,
        ip6_simple_entry_source_operations(),
    );
    fib.register_entry_source_behavior(
        FibEntrySourceBehaviorId::API,
        ip6_api_entry_source_operations(),
    );
    fib.register_entry_source_behavior(
        FibEntrySourceBehaviorId::INTERFACE,
        ip6_interface_entry_source_operations(),
    );
    fib.register_entry_source_behavior(
        FibEntrySourceBehaviorId::ADJACENCY,
        ip6_adjacency_entry_source_operations(),
    );
}
```

这些constructor返回各family单态化的`FibEntrySourceOperations<P,B,IpFibEntrySource>`；simple/API
共享一个private generic implementation，interface和adjacency也各自共享一个以backend operations
访问family事实的generic implementation。最终注册值仍分别是IPv4和IPv6 concrete static function
tables，不在dispatch时做family match。IP初始化必须在创建任何table entry之前完成两个family的
registration。

`FibSourceMain`同时显式注册本slice会使用的source metadata：service启动注册固定
`FibSource::INTERFACE`、`FibSource::API`和priority `0xd0`/ADJACENCY behavior的
`FibSource::ADJACENCY`；`Ip6LinkMain`初始化时调用`allocate("ip6-nd", 0xc1,
FibEntrySourceBehaviorId::API)`并私有保存返回identity。这里IP6-ND复用已经注册到`Ip6FibMain`的API
operations，不新增service source常量，也不新增IP6-ND behavior。

要为已有family Main的既定behavior注册或替换concrete implementation，插件必须显式依赖该family
owner并调用上述typed API，不能经`hammer-runtime`、`PluginMain`或service erased registry注入任意
state。

IP插件的`IpFibEntrySource`是VPP `fib_entry_src_t.u`在该concrete FIB中的typed Rust sum type。
其他插件若要给IP FIB增加新的source-private state，必须通过IP owner批准并扩展该enum及typed FES
operations；仅需既有simple/API/interface/adjacency行为的plugin只分配新`FibSource`并选择已注册
behavior。
这不会把IP source定义下沉到service，也不会让每条record携带type tag之外的erased storage。

required/optional规则跟VPP operations table一致：init/deinit若缺省为no-op，copy若缺省调用typed
`Clone::clone`；IP的`Copy` enum会被单态化为普通值复制，其他source definition可用自己的`Clone`语义，
不要求整个FIB source state为`Copy`。某action没有operation时走该action的VPP default结果，而不是返回自定义error。behavior
registration是void形状并替换该behavior slot，和VPP直接复制VFT一致；不增加duplicate-registration
error。source空间耗尽才返回`FibSource::INVALID`。operation返回后entry identity丢失是owner
invariant assertion。dispatch先复制对应function pointer、结束operations registry borrow，再用entry index和
source调用；不得同时把`&mut FibMain`及其内部record borrow传给callback，也不得让任何entry/source borrow
跨越可能扩容的registered operation。

`FibEntry.sources`是按priority排序的`Vec<FibEntrySrc<S,...>>`，source record直接包含source identity、
entry/source flags、refcount、path-list、path extensions和typed source state。查找使用source
identity；winner使用registry priority；operation通过source registry取得behavior id，再通过当前
`FibMain`取得operations。不得恢复按behavior分开的`Pool<FibEntrySrc>`、source-record index或
`source_references`/`source_forwarding`平行map。

interface FES是本ADR必须注册并验收的concrete实现：它只注册VPP `interface_src_vft`实际提供的
init/add/remove/path-swap/activate/deactivate/format/installed/cover-change slots，完成local source的
cover child与`PROVIDES_GLEAN`移交。reactivate、cover-update和forwarding-update slots保持`None`并使用
generic FES默认行为，不能为了复用代码私自增加interface callback。
IPv6-ND只是`Ip6LinkMain`注册的一个source identity，并选择已注册的API/simple behavior；它不是
service新增的FES enum variant。

ADJ FES同样是本ADR的必需实现，不等同于adjacency DPO operations。neighbor owner以
`FibSource::ADJACENCY`给host prefix增删attached path；ADJ FES维护自己的cover/sibling和每条path的
`REFINES_COVER` extension。activate查找less-specific cover并注册child；仅当cover当前为attached
（包括interface source的attached flags）且至少一条neighbor path的resolving interface等于cover
interface，或该path interface unnumbered到cover interface时，source才可安装。reactivate重新计算
refinement；deactivate/remove解除cover child和attached-export关系；installed负责attached export。
cover-change执行deactivate+activate，且仅重新允许install时返回`EVALUATE`；cover-update不重新计算
refinement，只读取当前cover winner的`ATTACHED`决定install，返回reason固定为`NONE`。缺少cover、extension或child identity属于与VPP `ASSERT`对应的owner
invariant，不新增route error。glean DPO由INTERFACE source的connected path解析产生，不使用ADJ FES；
ADJ FES只负责neighbor host-route source，二者不能混为一个“adjacency behavior”。

### 4.8 IPv6 link-local table owner

`Ip6LinkMain`增加按`sw_if_index`定位的private link-local FIB table owner。每个enabled link最多
一个当前link-local `/128` local contribution，forwarding为该interface/address的receive DPO。
对应VPP `FIB_SOURCE_IP6_ND`的`FibSource` identity由`Ip6LinkMain`启动时向`FibSourceMain`注册并
私有保存；generic service只定义source identity/registration与FES abstraction，不定义或导出
IPv6-ND source。

- first enable：创建或取得该interface的link-local table并安装当前`/128`；
- replace：在同一barrier scope内先删除旧`/128`，再安装新`/128`，最后发布新identity；
- last lock：删除当前`/128`；table无IP6-ND entries时释放该interface table；
- force interface delete：执行与last lock相同的route teardown，之后清除link record。

该table不是ordinary `Ip6Main.unicast_tables[fib_index]`，也不进入
`fib_index_by_sw_if_index`。本文不实现消费link-local table的ND/TEIB DPO packet path，但不能
因此省略VPP link owner本身必须维护的route事实。

## 5. Software-interface admin callback

### 5.1 Registration surface

`InterfaceRegistrationImage`和`declare_interface_registration_image!`增加第三个generic slice：

```rust
sw_interface_admin_up_down_callbacks: &'static [InterfaceCallbackRegistration]
```

`InterfaceState`拥有排序后的active vector。它与`sw_interface_callbacks`（create/delete）是不同
inventory；两者可复用相同function-pointer layout，但不能复用同一vector或把event type塞进
bool。`InterfaceCallbackRegistration.priority`使用service常量`INTERFACE_CALLBACK_PRIORITY_LOW = 0`和
`INTERFACE_CALLBACK_PRIORITY_HIGH = 1`；Hammer可使用稳定排序保持确定性，但任何owner和测试都不得依赖
同priority callback的相对顺序，VPP只给出LOW先于HIGH的合同。
service只知道interface、`DataPlaneMain`与up/down boolean，不出现IP类型或plugin引用。

IP image分别注册`ip4_sw_interface_admin_up_down`和`ip6_sw_interface_admin_up_down`两个LOW
priority callback。节点、callback和owner继续按family拆分；shared private core只接收具体
family table/address borrows。IP adjacency owner另注册glean LOW callback和neighbor HIGH callback；
二者保持独立walk，不能合并成一个遍历全部adjacency的callback。

### 5.2 Dispatch order

`InterfaceMain::set_software_flags`先验证interface与目标flags，然后把新flags写入live
`SwInterface`，使admin callback读取到通知中的新状态。IP callback拿到software
`sw_if_index`，不拿hardware index。

调用顺序保留VPP源码结构：

1. 暂时写入新software flags；
2. 执行按priority稳定排序的generic software admin callbacks；先完成IP4/IP6 route和glean等全部LOW，
   再执行neighbor HIGH；callback读取到新flags；
3. IP callbacks调用VPP形状的infallible route mutation并因registration ABI返回`Ok(())`；
4. generic callbacks全部成功后，执行现有device/hw-class callbacks；
5. generic callback返回错误时立即停止后续generic callbacks并恢复old flags；不反向重放此前
   已成功callback；
6. 后续device/hw-class callback返回错误时恢复old flags；不反向重放generic callbacks。

六步都直接对应VPP `vnet_sw_interface_set_flags_helper`和
`call_elf_section_interface_callbacks`：callback按priority执行，第一个error停止，owner只恢复
software flags。不得自行增加reverse callback/compensation语义。本文新增的两个IP callbacks在
FIB prerequisite gate通过后只执行不返回route error的FIB mutation并返回`Ok(())`。若排在其后的
generic或device/hw-class callback返回error，owner仍只恢复software flags，不撤销IP callback已完成
的route mutation；本文精确保留VPP这一非事务语义。

## 6. Ordinary route lifecycle

route mutation有两个独立入口，缺一不可：

| Trigger | Address state | Admin state | Required route action |
| --- | --- | --- | --- |
| ordinary address add成功 | 新地址已进入family address owner | admin-up | 立即调用family `add_interface_routes` |
| ordinary address add成功 | 新地址已进入family address owner | admin-down | 不添加route；地址保留，等待后续admin-up callback重放 |
| ordinary address delete成功 | 删除前已保留address、prefix length与fib index | admin-up | 立即调用family `del_interface_routes` |
| ordinary address delete成功 | 删除前已保留address、prefix length与fib index | admin-down | 不删除route；该状态下routes已由先前admin-down callback撤销 |
| admin-down -> admin-up | address owner中已有零个或多个地址 | 新状态为up | callback遍历全部已有地址并逐个add routes |
| admin-up -> admin-down | address owner中已有零个或多个地址 | 新状态为down | callback遍历全部已有地址并逐个del routes |

address add/delete路径必须自己读取`SwInterfaceFlags::ADMIN_UP`并作出判断，不能假设admin callback
会被同时触发；address mutation不改变admin flags，因此不会触发该callback。反过来，admin
callback只重放已经存在的address facts，不新增或删除address。两个入口调用同一组private family
route helpers，不能各自维护一套路由逻辑。

### 6.1 Address operation call order

IPv4 ordinary add/delete成功后的顺序：

1. address pool/hash/list mutation完成；
2. `ip4_sw_interface_enable_disable(sw_if_index, !is_delete)`；
3. 从`InterfaceMain`读取该`sw_if_index`当前software flags；若包含
   `SwInterfaceFlags::ADMIN_UP`，add调用`ip4_add_interface_routes`，delete调用
   `ip4_del_interface_routes`；否则明确跳过route mutation；
4. 按registration order调用IPv4 address callbacks。

IPv6 ordinary add/delete成功后的顺序：

1. address pool/hash/list mutation完成；
2. `ip6_sw_interface_enable_disable(sw_if_index, !is_delete)`；
3. add调用`ip6_link_enable`；
4. 从`InterfaceMain`读取该`sw_if_index`当前software flags；若包含
   `SwInterfaceFlags::ADMIN_UP`，add/del family routes；否则明确跳过route mutation；
5. 按registration order调用IPv6 address callbacks；
6. delete调用`ip6_link_disable`。

这里的admin判断属于每次address add/delete本身，不属于admin callback dispatch。admin-down时
跳过第3/4步route mutation，但仍保存/删除address、更新enable/link引用并通知address callbacks，
完全对应V1/V2。link-local public branch仍在ordinary pool前early return；
它只走第4.6节link-local route lifecycle，不进入本节ordinary route或tuntap callback。

### 6.2 Admin transition call order

`ip4_sw_interface_admin_up_down`与`ip6_sw_interface_admin_up_down`分别：

1. 从自己的family mapping取得`fib_index`；
2. 直接遍历该family `IpLookupMain`中该interface的ordinary address chain；
3. admin-up逐地址调用family add helper；admin-down逐地址调用family delete helper；
4. 不改变address pool、family enable refcount、IPv6 link lock或address callback list；
5. 返回`Ok(())`。

重复设置相同admin flags由`InterfaceMain`在dispatch前短路，因此不会重复增加prefix/FIB source
引用。interface delete现有顺序先把software flags置空，再执行software add/del callbacks：如果
原先admin-up，admin-down callback先撤销routes，后续address cleanup观察admin-down，只删地址
与引用，不二次删除routes。

## 7. Family route semantics

### 7.1 IPv4 add

对地址`A/L`、interface`I`、family table`F`：

1. 为`A/32`增加`FibSource::INTERFACE` local+connected contribution；path保存`A`与`I`并投影
   为receive DPO；
2. 若`L == 32`，结束；
3. 规范化`P = network(A/L)`，查找`(P, I)`prefix record；存在则仅增加refcount并结束；
4. 首次prefix创建record，并为`P/L`增加connected+attached contribution；backend在P2P interface
   投影zero-next-hop neighbor adjacency，在ordinary interface投影glean；
5. 若`L <= 30`：当network address不等于`A`时增加network `/32` drop；当broadcast address
   不等于`A`时增加broadcast `/32` drop + loose-uRPF-exempt；
6. 若`L == 31`：为`A xor 1`增加attached `/32`，path输入的next hop是peer address；resolver在
   P2P interface仍使用zero-next-hop neighbor，在ordinary interface使用peer neighbor。

同interface、同prefix、不同host address允许共存：每个host有独立local `/32`，prefix/special
routes只由prefix record共享。ADR-0028的overlap检查已禁止跨interface或不同prefix的歧义，route
helper不重复做address validation。

### 7.2 IPv4 delete

1. 删除`A/32`的interface-source local contribution；
2. `L == 32`则结束；
3. 查找`(network(A/L), I)`prefix record；缺失时记录warning并从prefix helper返回，不返回error、
   不panic；
4. 减少refcount；仍非零则结束；
5. `L <= 30`时先删除network/broadcast special `/32`；`L == 31`时删除peer attached `/32`；
6. 删除connected prefix contribution并释放prefix record。

这里精确保留VPP `clib_warning + return`。IPv4 local `/32`已在调用prefix helper前删除；prefix
record缺失时不再尝试special/connected route删除，也不把warning升级成address error或assertion。

### 7.3 IPv6 add/delete

IPv6 add：

1. 为`A/128`增加interface-source local+connected contribution并投影为receive DPO；
2. `L == 128`则结束；
3. 对`(network(A/L), I)`prefix record增加引用；首次引用安装connected+attached route，resolver按
   P2P/ordinary interface选择zero-next-hop neighbor或glean。

IPv6 delete保留VPP顺序：

1. `L < 128`时减少prefix refcount，最后引用删除glean prefix并释放record；
2. 删除`A/128` local contribution。

若IPv6 prefix record缺失，prefix helper记录warning并返回；外层route helper仍继续删除`A/128`
local contribution。该分支不返回error，也不panic。

IPv6没有IPv4 network/broadcast或`/31` special route。generic prefix refcount算法可共享，但
family add/delete orchestration保持两个具体函数。

## 8. 错误与同步语义

public address API仍只返回ADR-0028定义的六类VPP address error。VPP route helpers不向
`ip[46]_add_del_interface_address`增加errno；Hammer不得增加`RouteInstallFailed`、`Fib`、`Dpo`
或message-only address error。

FIB route mutation surface也按VPP返回形状，不定义`FibError`或backend error seam：

- `special_add`、`special_dpo_add/update`、`path_add`、`update`和`update_one_path`返回entry index；
- `special_remove`、`path_remove`和`delete`返回`()`；
- `fib_table_entry_delete`对不存在prefix幂等no-op；source remove对不存在source不返回错误或missing
  boolean；
- IPv4/IPv6 interface-prefix delete找不到prefix record时按第7节记录warning并返回；
- family interface route helpers为void形状，不产生route errno；admin callback只因现有registration
  ABI返回`InterfaceResult<()>`，正常IP路径固定返回`Ok(())`。
- node-type重复注册保持VPP assertion；FES behavior registration为void替换slot，不新增duplicate
  error；dynamic source identity耗尽返回`FibSource::INVALID`，不改成自定义allocation error。
- adjacency `add_or_lock`返回已有或新建pool index，不返回`AdjacencyError`；非法protocol、无效pool
  index、lock underflow、last-lock仍有children及删除不存在DB key都是owner invariant assertion，不能
  被改写成route/address API error；interface事件walk对不存在的per-interface DB直接no-op。

当前`FibError`和fallible backend projection属于未对齐的过渡实现，不进入目标API。DPO class、
Graph Node和family table必须在既有初始化顺序中存在；缺失属于对应owner初始化合同，不增加route
API error。Rust assertion只用于VPP源码已有`ASSERT`所表达的条件或其直接等价的pool/index owner
条件，不能把VPP的no-op或warning分支提升为panic。

route mutation遵循VPP各函数本身的顺序和引用生命周期；本文不增加跨route transaction、
pre-validation、candidate rollback或反向补偿合同。address callback仍在VPP规定的route helper
调用之后执行。

所有ordinary FIB、prefix、adjacency和link-local table mutation都由main thread在同一个现有
Worker Barrier interval内直接完成。不得给family tables或DPO pools增加`Mutex`、`RwLock`、
atomic pointer、snapshot handle、thread-local selector或第二套publication protocol。

## 9. API 与类型变更清单

### 9.1 新增

| Item | Visibility/owner | Responsibility |
| --- | --- | --- |
| `IpInterfacePrefix<P,S>`与prefix key/pool/hash | private, IP plugin | `(normalized prefix, sw_if_index)`共享route生命周期与`u32`refcount；IPv4 `S=u32`，IPv6 `S=()` |
| `ip4_add_interface_routes` / `ip4_del_interface_routes` | private, IP plugin | V3-V5全部IPv4 interface-source contributions |
| `ip6_add_interface_routes` / `ip6_del_interface_routes` | private, IP plugin | V6-V7 ordinary IPv6 contributions |
| `ip4_sw_interface_admin_up_down` / `ip6_sw_interface_admin_up_down` | private callbacks, IP plugin | admin transition遍历family address chain并add/del routes |
| `FibNodeType` / `FibNodePtr` / `FibNode` / `FibNodeOperations` | public generic infrastructure, service net | 12-byte embeddable FIB graph identity与可注册get/last-lock/back-walk/memory operations；public const constructor让plugin构造静态表并注册自己的concrete owner |
| `FibPathMode` | public generic FIB fact, service net | path fixup后的attached-next-hop/attached/receive/special分类；供generic comparator、resolver dispatch和back-walk使用，不含IP地址或runtime family |
| `FibNodeList` / `FibNodeSibling` / `FibNodeListMain` | public handles + service-owned pools | heterogeneous `(type,index)` child list与stable sibling unlink |
| `FibNodeBackWalkContext` / reason and walk flags / result / `FibWalkPriority` | public generic facts, service net | depth-bounded同步walk及main-thread HIGH/LOW async queue合同；不含plugin object/state或IP payload |
| `FibSourceMain` / `FibSource` / `FibEntrySourceBehaviorId` | public generic registry/identities, service net | source name、unique priority、behavior注册与plugin source allocation |
| `FibEntrySourceOperations<P,B,S>` / `FibMain::register_entry_source_behavior` | public generic value/API, service net | public typed slots与const empty constructor；FES action函数表只接收owner/entry-index/source及action facts；`S`由concrete FIB owner定义，不使用`dyn`或erased source data |
| `IpFibProtocol` / `IpAdjacencyLink` / `Ip46Address` / `IpFibPrefix` | private, IP plugin | 分离FIB protocol、link、DPO protocol语义；VPP IP46/fib-prefix fixed layout、family conversion、normalize与cover facts |
| `IpFibEntrySource`与IP FES operations constructors/registration functions | private, IP plugin | `FibEntrySrc`的concrete source state、simple/API/interface/ADJ静态operations values及两个family的真实registration；interface与ADJ variants各自保存cover/sibling |
| IP adjacency path-extension variant | private, IP plugin | ADJ FES逐path保存`REFINES_COVER`并随path-list copy-on-write重新resolve；不是service-owned IP flag |
| `IpAdjacencySubtype` / `IpAdjacencyRewrite` / `IpAdjacency` | private, IP plugin | 一个VPP-shaped adjacency object layout；neighbor/glean subtype、rewrite和第4 cache-line控制字段 |
| `IpAdjacencyMain`与四个family/class nodes | private, IP plugin | 单一adjacency pool/hash/lock lifetime；IPv4/IPv6 glean与incomplete nodes分别注册 |
| per-interface IPv6 link-local FIB owner | private, `Ip6LinkMain` | plugin-private IPv6-ND source的`/128` local route与table lifetime |

### 9.2 修改

| Item | Change |
| --- | --- |
| `IpLookupMain` | 增加prefix pool/hash；不增加classify、MFIB或generic interface state |
| `Ip4Main` / `Ip6Main` family tables | 允许main-thread barrier内private direct mutation；worker API仍只读 |
| `InterfaceRegistrationImage`与declaration macro | 增加独立`sw_interface_admin_up_down_callbacks` slice |
| `InterfaceState` / `InterfaceMain::set_software_flags` | owner安装/排序/dispatch generic admin callbacks，并遵守第5.2节VPP error顺序 |
| `InterfaceMain` | 增加只读`is_p2p(sw_if_index)`，从software interface定位hardware class并读取现有`HwClassFlags::P2P`；不新增IP或FIB能力 |
| interface MTU callback inventory | 增加独立software-interface MTU change callback；`set_mtu`写入新MTU后通知，adjacency owner按VPP generic walk只更新neighbor并back-walk；不复用admin/create-delete callback |
| ADR-0028 family address operations | 在enable/link与address callback之间加入第6.1节family route helper调用 |
| `Ip6LinkMain` | link identity/locks之外增加link-local table route owner |
| `hammer-service::net::dpo` | 删除没有VPP adjacency layout/owner语义的泛型`AdjacencyDpo<A,R>`；DPO registry仍只操作`DpoId`和owner注册的静态operations |
| `FibTable` live mutation path | 完成ADR-0005已批准的`special_add/remove`、`special_dpo_add/update`、`path_add/remove`、`update/update_one_path`与`delete`语义；接通现有`FibEntrySrc`/`FibPathList`/node-backed `FibPath`，淘汰当前直接DPO的`add_route/remove_route`过渡模型 |
| `FibSource` / `FibEntrySrc` | `FibSource`改为registry identity；entry直接保存priority-sorted source records；删除封闭`FibSourceBehavior` dispatch、按behavior source pools及平行source maps |
| `Ip4FibBackend` / `Ip6FibBackend` | 分别实现具体family path fixup、path resolution、稳定LB/uRPF原地projection与forwarding store publication；保持同一个泛型`FibMain<P,B,S>`抽象，不新增backend seam |
| path comparator | 删除family backend的`compare_paths` seam；service generic FIB按preference、path type/protocol及`N: Ord`字段实现一次VPP比较顺序，编译器分别为IPv4/IPv6单态化 |

### 9.3 目标 Rust 类型与方法

本节是实现签名合同，不是示意性的另一套模型。`FibPath`、`FibEntrySrc`、`FibPathList`、
`FibTable`和`FibTableBackend`都是现有类型；实现只补全它们已经由ADR-0005批准的职责。新增的
`IpInterfacePrefix<P,S>`、`IpFibPrefix`和`IpAdjacency`只属于IP plugin。

泛型FIB增加由concrete FIB owner提供的source-state参数`S`，不增加runtime family enum或trait
object。现有`FibPath`升级为live
node-backed path，而不是再新增一套route-path wrapper：

```rust
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FibPathMode {
    AttachedNextHop = 0,
    Attached = 1,
    Special = 3,
    Receive = 8,
}

pub struct FibPath<N, F> {
    node: FibNode,
    path_list_index: u32,
    sibling: Option<FibNodeSibling>,
    forwarding: Option<DpoId>,
    mode: FibPathMode,
    pub sw_if_index: u32,
    pub table_id: u32,
    pub rpf_id: u32,
    pub weight: u8,
    pub preference: u8,
    pub flags: F,
    pub next_hop: N,
}

pub struct FibEntrySrc<S, N, F, PathExt> {
    path_exts: FibPathExtList<N, F, PathExt>,
    path_list: Option<u32>,
    entry_flags: FibEntryFlags,
    source: FibSource,
    flags: FibEntrySrcFlags,
    ref_count: u8,
    source_state: S,
}

impl<S, N, F, PathExt> FibEntrySrc<S, N, F, PathExt> {
    pub fn source(&self) -> FibSource;
    pub fn source_state(&self) -> &S;
    pub fn source_state_mut(&mut self) -> &mut S;
}

pub struct FibPathList<N, F> {
    node: FibNode,
    paths: Vec<u32>, // indices in FibMain.paths
    key_flags: FibPathListFlags,
    flags: FibPathListFlags,
    urpf_index: Option<u32>,
}

pub struct FibEntry<P, N, F, S, PathExt> {
    node: FibNode,
    fib_index: u32,
    prefix: P,
    flags: FibEntryFlags,
    sources: Vec<FibEntrySrc<S, N, F, PathExt>>,
    parent: Option<u32>,
    sibling: Option<FibNodeSibling>,
    forwarding: Option<DpoId>,
}

pub struct FibMain<P, B, S>
where
    P: Copy + Ord,
    B: FibTableBackend<Prefix = P>,
    S: Default + Clone,
{
    tables: Vec<FibTable<P, B>>,
    entries: Pool<FibEntry<
        P,
        B::NextHop,
        B::PathFlags,
        S,
        B::PathExtension,
    >>,
    entry_source_operations:
        Vec<Option<FibEntrySourceOperations<P, B, S>>>,
    path_lists: Pool<FibPathList<B::NextHop, B::PathFlags>>,
    paths: Pool<FibPath<B::NextHop, B::PathFlags>>,
    entry_node_type: FibNodeType,
    path_list_node_type: FibNodeType,
    path_node_type: FibNodeType,
}

pub struct FibTable<P, B> {
    backend: B,
    prefixes: BTreeMap<P, u32>,
}
```

`FibPathMode`只列出本文会构造的VPP path types，但显式discriminant保留VPP
`fib_path_type_t`中的0/1/3/8排序值；后续增加recursive/exclusive等mode时必须使用对应VPP排序位置，
不能因Rust enum当前variant较少而重新编号。

`FibEntrySrc`的`S`不是backend的associated `SourceData`，也不是service预定义union；它就是
concrete FIB owner指定的source definition/state。IP插件的闭合定义至少包含以下状态：

```rust
#[derive(Clone, Copy, Default)]
enum IpFibEntrySource {
    #[default]
    Stateless,
    Interface {
        cover: Option<u32>,
        sibling: Option<FibNodeSibling>,
    },
    Adjacency {
        cover: Option<u32>,
        sibling: Option<FibNodeSibling>,
    },
}
```

SIMPLE/API init保留`Stateless`；INTERFACE与ADJACENCY init分别建立对应variant。ADJACENCY path
refinement不塞入source state，而由IP插件的`B::PathExtension` sum type为每条path保存
`Adjacency { refines_cover: bool }`，对应VPP `FIB_PATH_EXT_ADJ`。`source_state_mut`只供已注册的typed
FES operation修改当前record；route helper不能直接借用或修改它。`S: Default + Clone`使source定义
控制缺省copy语义；IP的`Copy` enum clone会被内联成VPP union memcpy等价操作，拥有引用生命周期的
source state必须在自己的`Clone`以及必要的copy/deinit operations中维护该生命周期。

三个node-type值由IP插件启动时连同`FibNodeOperations`注册并直接传给`FibMain::new`；不新增仅用于
搬运它们的wrapper，也不把plugin pool/reference放进service。由于Rust按family单态化pool，IPv4和
IPv6分别注册自己的三个type；registered static thunks由type选择对应family pool。这替代VPP C中
一个IP46全局path pool，不在packet path增加runtime family分支。

`FibEntrySrc`、`FibPathList`与live path字段由FIB owner私有；测试通过窄查询方法读取事实，不让
IP helper直接改flags、refcount、behavior data、path-list index或uRPF index。source behavior经
registered operations dispatch；`FibEntry.sources`直接保存record并保持priority顺序，不再解释
behavior-local pool index。

`FibPath::new`构造尚未入pool、未持有DPO/child的path facts；`FibMain`完成fixup/sort后才把它移入
family-global path pool并解析。live path不可`Clone`/`Copy`，避免复制DPO引用和sibling；
`FibPathList`共享的是stable path indices。每个live path保存不可变的`path_list_index`；每个active
entry保存winner path-list的`parent`与`sibling`。path-list的全部source/child ownership只计入embedded
`FibNode.lock_count`，不存在第二个source counter。backend通过
`FibPath::{path_list_index,forwarding,sibling,node}`窄getter读取，不取得pool或字段的可变逃逸引用。

唯一静态backend seam补足具体family实现所需的直接借用。这里不增加projection input wrapper：

```rust
pub trait FibTableBackend {
    type Prefix: Copy + Ord;
    type PacketAddress: Copy;
    type NextHop: Clone + Ord;
    type PathFlags: Copy + Ord;
    type PathExtension;

    fn lookup(&self, prefix: Self::Prefix) -> Option<u32>;
    fn lookup_exact(&self, prefix: Self::Prefix) -> Option<u32>;
    fn less_specific(&self, prefix: Self::Prefix) -> Option<(Self::Prefix, u32)>;
    fn insert_entry(&mut self, prefix: Self::Prefix, entry: u32);
    fn remove_entry(&mut self, prefix: Self::Prefix, entry: u32);

    fn forwarding_lookup(&self, address: Self::PacketAddress) -> Option<DpoId>;
    fn forwarding_update(&mut self, prefix: Self::Prefix, dpo: DpoId);
    fn forwarding_remove(
        &mut self,
        prefix: Self::Prefix,
        old: DpoId,
        cover: Option<(Self::Prefix, DpoId)>,
    );

    fn path_fixup(
        &mut self,
        prefix: Self::Prefix,
        entry_flags: &mut FibEntryFlags,
        path: &mut FibPath<Self::NextHop, Self::PathFlags>,
    );

    fn contribute_urpf_interfaces(
        &self,
        path: &FibPath<Self::NextHop, Self::PathFlags>,
        interfaces: &mut Vec<u32>,
    );

    fn resolve_path(
        &mut self,
        main: &mut DataPlaneMain,
        path_index: u32,
        path: &FibPath<Self::NextHop, Self::PathFlags>,
    ) -> (DpoId, Option<FibNodeSibling>);

    fn unresolve_path(
        &mut self,
        main: &mut DataPlaneMain,
        path_index: u32,
        path: &FibPath<Self::NextHop, Self::PathFlags>,
    );

    fn update_forwarding(
        &mut self,
        main: &mut DataPlaneMain,
        prefix: Self::Prefix,
        source: FibSource,
        entry_flags: FibEntryFlags,
        path_list: Option<&FibPathList<Self::NextHop, Self::PathFlags>>,
        glean_source: Option<Self::Prefix>,
        special_dpo: Option<DpoId>,
        entry_lb: &mut Option<DpoId>,
    );
}
```

`path_fixup`、`contribute_urpf_interfaces`、path resolve/unresolve和LB bucket生成由两个IP backend分别
实现。path comparator由generic FIB实现一次：先比较preference，再比较`FibPathMode`及该mode对应的
next-hop/interface/table/flags字段；next-hop protocol由当前family Main静态固定，`NextHop: Ord`让
IPv4/IPv6地址比较单态化，不在运行时match family。generic FIB负责稳定排序、去重、
pool ownership和调用时序。
resolve为path取得一个DPO引用；若child是adjacency，同时调用owner的child-add并返回sibling。
unresolve按相反顺序child-remove再释放DPO，且不返回route error。`glean_source`是generic cover graph选出的provider prefix，backend才把它解释为
IPv4或IPv6 source address。`special_dpo`是non-owning identity input。`update_forwarding`只允许在
`entry_lb == None`时创建一个LB并写入；`Some`时必须通过现有load-balance owner原地更新同一index的
buckets、flags和uRPF，禁止替换identity。generic FIB只在首次install把该LB插入forwarding trie，
uninstall时才remove/reset。backend方法不增加VPP route mutation不存在的可恢复错误。

`insert_entry`、`remove_entry`、`forwarding_update`与`forwarding_remove`由generic table在已判定
对应entry状态的分支调用，因此同样不返回backend `Result`。当前backend的`PrefixExists`、
`PrefixMissing`与`ForwardingDpoRequired`不保留为目标route API error；entry存在性由VPP形状的
table operation决定，forwarding DPO类型由FIB projection构造。

VPP形状的methods由family-global `FibMain`执行并显式接收`fib_index`，替代当前直接接收LB的
`add_route/remove_route`过渡入口：

```rust
impl FibSourceMain {
    pub fn register(
        &mut self,
        source: FibSource,
        name: &str,
        priority: u8,
        behavior: FibEntrySourceBehaviorId,
    );

    pub fn allocate(
        &mut self,
        name: &str,
        priority: u8,
        behavior: FibEntrySourceBehaviorId,
    ) -> FibSource;
}

impl<P, B, S> FibMain<P, B, S>
where
    P: Copy + Ord,
    B: FibTableBackend<Prefix = P>,
    S: Default + Clone,
{
    pub fn register_entry_source_behavior(
        &mut self,
        behavior: FibEntrySourceBehaviorId,
        operations: FibEntrySourceOperations<P, B, S>,
    );

    pub fn special_add(
        &mut self,
        main: &mut DataPlaneMain,
        fib_index: u32,
        prefix: P,
        source: FibSource,
        flags: FibEntryFlags,
    ) -> u32;

    pub fn special_dpo_add(
        &mut self,
        main: &mut DataPlaneMain,
        fib_index: u32,
        prefix: P,
        source: FibSource,
        flags: FibEntryFlags,
        dpo: DpoId,
    ) -> u32;

    pub fn special_dpo_update(
        &mut self,
        main: &mut DataPlaneMain,
        fib_index: u32,
        prefix: P,
        source: FibSource,
        flags: FibEntryFlags,
        dpo: DpoId,
    ) -> u32;

    pub fn special_remove(
        &mut self,
        main: &mut DataPlaneMain,
        fib_index: u32,
        prefix: P,
        source: FibSource,
    );

    pub fn update(
        &mut self,
        main: &mut DataPlaneMain,
        fib_index: u32,
        prefix: P,
        source: FibSource,
        flags: FibEntryFlags,
        paths: Vec<FibPath<B::NextHop, B::PathFlags>>,
    ) -> u32;

    pub fn update_one_path(
        &mut self,
        main: &mut DataPlaneMain,
        fib_index: u32,
        prefix: P,
        source: FibSource,
        flags: FibEntryFlags,
        path: FibPath<B::NextHop, B::PathFlags>,
    ) -> u32;

    pub fn path_add(
        &mut self,
        main: &mut DataPlaneMain,
        fib_index: u32,
        prefix: P,
        source: FibSource,
        flags: FibEntryFlags,
        paths: Vec<FibPath<B::NextHop, B::PathFlags>>,
    ) -> u32;

    pub fn path_remove(
        &mut self,
        main: &mut DataPlaneMain,
        fib_index: u32,
        prefix: P,
        source: FibSource,
        paths: &[FibPath<B::NextHop, B::PathFlags>],
    );

    pub fn delete(
        &mut self,
        main: &mut DataPlaneMain,
        fib_index: u32,
        prefix: P,
        source: FibSource,
    );
}
```

`update`消费`Vec`，使FIB可原地fixup/sort后把同一allocation移入结果path-list；
`update_one_path`走同一实现，不复制第二套逻辑。`path_remove`只匹配现有path，因此直接借用slice。
`delete`、`path_remove`和`special_remove`对不存在prefix幂等no-op；`path_remove`对不存在source
也直接返回。`special_remove`/`delete`的source remove action不暴露error或missing boolean，并保留
VPP special refcount语义。直接DPO方法只允许owner-internal special contribution使用，ordinary IP
route和interface route都不能调用。`special_dpo_add/update`借用caller的DPO identity并为source
取得自己的引用；caller ownership不转移，replacement按source生命周期释放旧引用。

两个具体实例继续是现有aliases；实现中不出现`IpAddr`或runtime family match：

```rust
pub type Ip4FibMain = FibMain<Ipv4Net, Ip4FibBackend, IpFibEntrySource>;
pub type Ip6FibMain = FibMain<Ipv6Net, Ip6FibBackend, IpFibEntrySource>;

impl FibTableBackend for Ip4FibBackend {
    type Prefix = Ipv4Net;
    type PacketAddress = Ipv4Addr;
    type NextHop = Ipv4Addr;
    type PathFlags = IpPathFlags;
    // IPv4-only fixup, resolution and forwarding store.
}

impl FibTableBackend for Ip6FibBackend {
    type Prefix = Ipv6Net;
    type PacketAddress = Ipv6Addr;
    type NextHop = Ipv6Addr;
    type PathFlags = IpPathFlags;
    // IPv6-only fixup, resolution and forwarding store.
}
```

prefix refcount算法由一个private泛型实现共享，family orchestration仍是四个具体函数：

```rust
impl<A, P, S> IpLookupMain<A, P, S>
where
    A: Copy + Eq + Hash,
    P: Copy + Eq + Hash,
    S: Copy,
{
    fn lock_interface_prefix(
        &mut self,
        prefix: P,
        sw_if_index: u32,
        source: S,
    ) -> (u32, bool); // (pool index, first reference)

    fn unlock_interface_prefix(
        &mut self,
        prefix: P,
        sw_if_index: u32,
    ) -> bool; // true only for the final reference
}

fn ip4_add_interface_routes(
    main: &mut DataPlaneMain,
    lookup: &mut IpLookupMain<Ipv4Addr, Ipv4Net, u32>,
    fib: &mut Ip4FibMain,
    fib_index: u32,
    sw_if_index: u32,
    if_address_index: u32,
);

fn ip4_del_interface_routes(
    main: &mut DataPlaneMain,
    lookup: &mut IpLookupMain<Ipv4Addr, Ipv4Net, u32>,
    fib: &mut Ip4FibMain,
    fib_index: u32,
    sw_if_index: u32,
    address: Ipv4Addr,
    address_length: u8,
);

fn ip6_add_interface_routes(
    main: &mut DataPlaneMain,
    lookup: &mut IpLookupMain<Ipv6Addr, Ipv6Net, ()>,
    fib: &mut Ip6FibMain,
    fib_index: u32,
    sw_if_index: u32,
    if_address_index: u32,
);

fn ip6_del_interface_routes(
    main: &mut DataPlaneMain,
    lookup: &mut IpLookupMain<Ipv6Addr, Ipv6Net, ()>,
    fib: &mut Ip6FibMain,
    fib_index: u32,
    sw_if_index: u32,
    address: Ipv6Addr,
    address_length: u8,
);

fn ip4_sw_interface_admin_up_down(
    main: &mut DataPlaneMain,
    interfaces: &InterfaceMain,
    sw_if_index: u32,
    is_up: bool,
) -> InterfaceResult<()>;

fn ip6_sw_interface_admin_up_down(
    main: &mut DataPlaneMain,
    interfaces: &InterfaceMain,
    sw_if_index: u32,
    is_up: bool,
) -> InterfaceResult<()>;
```

add helper使用仍在pool中的`if_address_index`；delete helper只接收删除后仍有效的address facts，
不借用已释放pool slot。四个route helper的commit path不返回route error，符合VPP的void helper；
两个admin callback的`InterfaceResult`仅为registration ABI，完成FIB gate后正常IP路径固定返回
`Ok(())`。

### 9.4 不新增/不复用

- 不新增public route helper、`IpFamilyMain`、`Ip46Route`、`InterfaceRouteManager`或统一family node；
- 不把IP route callback塞入device/hw-class callback字段；
- 不把`InterfaceRxDpo`复用为receive、glean或attached adjacency；
- 不在`hammer-service`定义或导出`AdjacencyDpo`、`IpFibPrefix`、`IpAdjacency`或IPv6-ND source；
- 不在service `FibNodeMain`保存plugin Main引用、object引用或type-erased plugin state；只保存
  `FibNodeOperations`静态函数与type name/id，concrete pool仍归注册插件；
- 不用封闭`FibSourceBehavior` match、`dyn` trait object或按behavior分池代替可注册FES operations；
- 不为IPv4/IPv6建立不同adjacency object pool；只拆Graph Node和typed family入口；
- 不以drop route代替ordinary-interface glean或P2P zero-neighbor，也不以空`DpoId`声称route已安装；
- 不让`InterfaceMain`保存IP prefix、FIB table、address或DPO；
- 不新增public address error variant；
- 不在`hammer-service::net::fib`增加任何IPv4/IPv6 address、prefix、path flag、adjacency或interface-address policy；
- 不实现本文范围外的classify、MFIB、neighbor resolution或directed-broadcast control API。

## 10. 三向语义差异

| Dimension | 当前 Hammer | 本文目标 | vendored VPP | 结论 |
| --- | --- | --- | --- | --- |
| address成功路径 | address/enable/callback，无route | admin-up时同步提交family routes后callback | V1-V2 | 必须修复 |
| admin callback | 只有device/hw class | 独立generic software admin inventory + 两个family callbacks | V8-V9 | owner/category对齐；成功路径状态一致 |
| callback相对device order | device/hw only | generic callback先于device/hw；第一个error停止并只恢复flags | generic callback先于device/hw；第一个error停止并只恢复flags | 顺序与错误语义对齐，不增加reverse callback |
| FIB抽象边界 | generic types存在，live table仍绕过source/path graph | service net完成泛型source/entry/path生命周期；IPv4/IPv6 backend分别解释具体path并投影 | `fib_entry`/`fib_entry_src`/`fib_path_list`通用图 + IP protocol具体path/DPO实现 | 静态泛型边界对齐，不下沉IP事实 |
| prefix owner | 无 | family lookup main pool/hash/refcount | V4,V6,V15 | owner与共享生命周期对齐 |
| local route | 无 | receive DPO + LB + interface source | V3,V6,V10-V11 | 对齐 |
| connected route | 无 | P2P（tuntap）zero-neighbor或ordinary-interface glean + LB + interface source | V4,V6,V10-V12,V32 | 对齐resolver分支；neighbor完成事件延期 |
| IPv4 special hosts | 无 | network/broadcast drop与`/31`peer attached | V4,V5,V13 | 默认directed-broadcast-disabled语义对齐 |
| IPv6 link-local route | 只有identity/locks | per-interface LL FIB `/128` local source | V14 | owner/lifecycle对齐；ND packet consumers延期 |
| route error | 当前直接DPO入口使用fallible backend/FIB seam | FIB add/update返回entry index，remove/delete返回`()`；missing prefix删除no-op，missing source不产生error/missing boolean；prefix record缺失warning+return；interface helper不产生route errno | VPP `fib_table_entry_*`与interface route helper采用相同返回形状 | 删除自定义`FibError`、rollback与panic语义 |

## 11. 两阶段聚焦测试矩阵

### 11.1 FIB prerequisite gate

下列测试先于任何interface-route实现完成；任一失败都禁止进入第二阶段：

| Test | Level | Required assertions | Evidence |
| --- | --- | --- | --- |
| `fib_source_and_fes_registration_dispatch` | service generic FIB + IP init | fixed/dynamic source注册得到class+slot唯一priority；IP4/IP6初始化真实调用各自registration function并填入SIMPLE/API/INTERFACE/ADJACENCY slots；source metadata lookup得到behavior，behavior slot得到注册的`FibEntrySourceOperations<P,B,S>`，再命中对应static function；callback只接收main/entry-index/source，测试在callback内触发entry/path pool扩容后仍按index取得正确record；INTERFACE未注册的reactivate/cover-update/fwd-update走generic default；只声明泛型实现但未调用registration不能被dispatch；behavior re-register替换slot且source耗尽返回`INVALID`；全程无`dyn`/closure/erased state | V16,V28,V33,V36-V37 |
| `adjacency_source_tracks_cover_and_refinement` | generic graph + IP FES | `FibSource::ADJACENCY` metadata为priority `0xd0`和ADJACENCY behavior；neighbor host path创建ADJ source record；相同或unnumbered resolving interface设置`REFINES_COVER`并允许安装，不匹配时不安装；path add/remove/swap更新extension；deactivate/remove解除cover sibling与attached export；cover-change执行重新跟踪且只在可安装时返回`EVALUATE`，cover-update只读`ATTACHED`且reason为`NONE` | V33,V37 |
| `fib_source_winner_uses_live_source_records` | service generic FIB | `FibEntry.sources`直接保存priority-sorted `FibEntrySrc`；`INTERFACE(0x03)`胜过`API(0x80)`；loser path-list保留；winner删除后activate loser；entry flags始终来自winner | V16,V18,V29 |
| `fib_node_list_tracks_children_and_last_lock` | service net FIB graph | 两个plugin-owned test node types注册各自operations；child-add经parent type定位owner、创建list并增加lock；sibling删除修复双链；last child触发registered last-lock；iterator在pool扩容后仍按type/index工作；async walk以internal node及两个siblings锁住parent并排队，按HIGH再LOW推进，完成/取消/shutdown均解除两个关系且不遗留node lock | V25,V34,V42 |
| `fib_back_walk_reaches_entry_and_updates_forwarding` | generic FIB + IP backend | live path有family-global stable index和所属path-list index；glean/neighbor resolve登记adjacency child与sibling；winner entry以parent/sibling成为path-list child；`INTERFACE_UP/DOWN/DELETE`、`ADJ_UPDATE/DOWN/MTU`从adjacency到path、path-list、entry；path按reason更新resolved/DPO，path-list重建uRPF；entry对VPP列出的evaluate/adjacency/interface reasons reactivate，但`ADJ_MTU`不reactivate，随后一律向children改发`EVALUATE`；admin-down `FORCE_SYNC`到首个entry后清除；re-resolve/delete先解除child再释放DPO | V26,V31,V34 |
| `fib_update_and_special_reference_semantics_are_distinct` | service generic FIB | `update_one_path`替换完整path set且不增加special refcount；`special_add`只在0->1创建、重复add增加refcount、1->0销毁；missing prefix的remove/delete为no-op；missing source不产生error或missing boolean | V17,V18,V23 |
| `fib_path_lists_share_and_copy_on_write` | service generic FIB | generic comparator按preference/type/protocol/next-hop字段稳定排序；相同paths/key flags共享list；修改一个source产生replacement list且不改变另一source；source lock与entry child都计入embedded node lock，没有`source_count`；第64个child永久设置`POPULAR`，普通walk同步、popular walk排LOW async、`FORCE_SYNC`覆盖到首个entry；last node lock统一销毁paths/uRPF | V17,V19,V34,V40-V41 |
| `fib_entry_keeps_stable_load_balance` | generic FIB + concrete backend | 首次install为entry创建一个LB并插入trie；winner/path/uRPF/loop变化原地更新同一DPO class/index及其引用；loser与共享path-list仍有效；只有uninstall从trie移除并reset，禁止replacement root | V18-V20,V35 |
| `ip4_fib_projects_paths_and_urpf` | IPv4 concrete backend | local path得到receive；connected+attached在P2P得到zero-neighbor、ordinary interface得到glean；attached host按同一P2P规则解析；interface/adjacency down时path unresolved；attached path仍把interface烘焙进uRPF | V10-V12,V20,V22,V32 |
| `ip6_fib_projects_paths_and_urpf` | IPv6 concrete backend | 与IPv6 concrete types对应的receive；P2P zero-neighbor与ordinary glean分支；attached与uRPF projection；测试不经过IPv4分支或runtime family enum | V10-V12,V20,V22,V32 |
| `ip_adjacency_layout_and_shared_pool_match_vpp` | IP adjacency owner | `IpFibPrefix` size/offset、`FibNode` size/offset、48-byte subtype、128-byte rewrite、`IpAdjacency` align 64/size 256，以及delegates/node/control/padding精确offset；IP4/IP6 glean和neighbor DPO index均指向同一个pool；glean/incomplete创建通过`stack_from_node`得到指向对应interface TX node的真实`rewrite.next_index` | V24-V27,V30,V39 |
| `ip_adjacency_databases_match_vpp_keys` | IP adjacency owner | glean按protocol/interface分区且只用normalized address查找；不同prefix length不被私自加入key；neighbor按nh-protocol/interface分区且key含address+64-bit link；last-lock以同一key删除 | V12,V30 |
| `ip_adjacency_events_and_last_lock_walk_children` | IP/service integration | IP4/IP6 route及glean callbacks为LOW，neighbor为HIGH，dispatch观测到全部LOW先于HIGH；MTU只walk neighbor并以`ADJ_MTU`通知，glean不收到该事件；neighbor down强制sync且临时自锁；glean/neighbor admin/delete分别以`INTERFACE_UP/DOWN/DELETE`通知；interface add不resolve旧对象；最后path解除后按subtype删除DB key并释放slot | V25-V27,V31,V38 |
| `interface_source_tracks_cover_and_glean_provider` | generic graph + concrete family backend | local source跟踪less-specific connected cover；首个local成为`PROVIDES_GLEAN`；删除、winner或cover变化后迁移到仍有效local；cover/sibling无泄漏 | V21 |

这些测试必须通过真实`FibMain<P,B,S>` mutation观察entry/source/path-list、backend forwarding与DPO
生命周期。单独new一个`FibPathList`再手工绑定LB的primitive测试不能替代gate。

### 11.2 Interface route consumer

| Test | Level | Required assertions | Evidence |
| --- | --- | --- | --- |
| `interface_routes_follow_address_and_admin_state` | IP/service integration | admin-down add保存address但不产生interface-source route；up安装；down撤销且address保留；再次up恢复；相同flags短路不重复引用 | V1,V2,V8,V9 |
| `ip4_interface_routes_match_vpp_prefix_cases` | IP FIB/DPO integration | ordinary interface `/24`产生local receive、connected glean、network/broadcast drop；P2P tuntap `/24`的connected为zero-neighbor；同prefix ref共享正确；`/31`普通/P2P neighbor key不同；`/32`只local；missing prefix warning+return | V3-V5,V10-V13,V23,V32 |
| `ip6_interface_routes_match_vpp_prefix_cases` | IP FIB/DPO integration | ordinary interface `/64`产生local receive与glean，P2P tuntap产生zero-neighbor；prefix ref/delete顺序正确；`/128`只local；missing prefix warning+return且外层仍删除local | V6,V7,V10-V12,V23,V32 |
| `ip6_link_local_route_follows_link_lifetime` | IP link/FIB integration | first enable创建per-interface table与`/128` local；replace撤销旧route并安装新route且不增加link lock；last lock/force delete删除route并释放空table | V14 |

两阶段测试都读取真实owner/FIB/DPO和callback可观察状态，不读取`.rs`文本，不通过symbol-name
`contains`声称行为存在。privileged tuntap lab不属于本文。

## 12. 实现顺序与完成判定

实现顺序：

1. 在`hammer-service::net`增加`fib_node`/`fib_node_list`和process-global list pools，完成
   `FibNodeOperations` type registration、child/sibling、lock/last-lock与back-walk dispatch；
2. 在service定义`FibSourceMain`，启动时至少注册INTERFACE/API/ADJACENCY fixed source metadata；把generic FIB改为
   `FibMain<P,B,S>`和`FibEntrySrc<S,...>`，由concrete owner定义`S`；service不定义public FES trait；
3. 构造`Ip4FibMain`/`Ip6FibMain`后，IP初始化必须分别调用`register_ip4_fib_entry_sources`和
   `register_ip6_fib_entry_sources`，把SIMPLE/API/INTERFACE/ADJACENCY的concrete
   `FibEntrySourceOperations<P,B,S>`写入behavior slots；注册完成前不得创建table entry或route；
4. 完成table共享stable entry/path-list/path pools，entry直接拥有priority-sorted
   `FibEntrySrc<IpFibEntrySource,...>` records；path保存所属path-list，active entry以parent/sibling成为
   winner path-list child，接通`adjacency -> path -> path-list -> entry` back-walk；之后
   `Ip6LinkMain`才可分配选择已注册API behavior的plugin-private IP6-ND source identity；
5. 在IP插件实现VPP-shaped `IpFibPrefix`、单一`IpAdjacency` pool、完整rewrite layout、DB、
   family node IDs、DPO operations、真实interface-TX next slot、path child和MTU back-walk；
6. 完成ADR-0005已批准的generic source/path operations、winner、cover，以及entry-owned稳定LB的首次
   install、原地update和uninstall生命周期；
7. 在`Ip4FibBackend`与`Ip6FibBackend`中分别完成具体family path fixup、receive/glean/
   attached-neighbor resolution、LB bucket/uRPF原地projection和forwarding publication；generic
   comparator只在service实现一次，两个backend不复制排序算法；
8. FIB实现形成独立commit后，只运行第11.1节FIB最终gate；通过后立即提交该FIB
   commit，并审查确认service抽象没有IP事实、两个family backend没有复制generic graph逻辑；
   该commit是硬gate，提交前不开始interface route实现；
9. gate关闭后，给`IpLookupMain`增加prefix owner；
10. 实现IPv4和IPv6各自的private add/del route helpers；helpers只提交path/special facts；
11. 增加generic software admin和MTU callback inventory并迁移registration image；IP4/IP6 route及
    glean注册LOW，neighbor注册HIGH；
12. 把family helpers接入address成功路径和两个family admin callbacks；
13. 给`Ip6LinkMain`接入per-interface link-local table route；
14. 运行第11.2节interface-route聚焦测试，最终gate通过后提交。

完成判定：

- generic FIB live table不再使用平行source reference/forwarding maps代替`FibEntrySrc`和path-list；
- IPv4/IPv6只通过同一个`FibMain<P,B,S>`抽象共享图逻辑，各自在concrete backend解释family path，
  `S=IpFibEntrySource`仍归IP插件定义；
- Fib node type与FES behavior均由插件把静态operations value显式注册进对应registry；仅有trait或
  generic implementation不算完成，service不保存plugin object/state，FIB不使用封闭behavior match；
- 两个family behavior registry都存在ADJACENCY operations；`FibSource::ADJACENCY`经source metadata
  命中该slot，cover/sibling、path refinement和attached-export lifecycle不由adjacency DPO代替；
- FES callback只使用稳定entry index/source identity，pool扩容前结束内部borrow并在返回后重新取得；
- adjacency事件沿path、path-list到entry，entry forwarding在installed生命周期保持同一LB identity；
- path-list只有embedded node lock一个引用权威，generic comparator没有IPv4/IPv6重复实现；
- adjacency rewrite next slot来自现有DPO/graph stack到interface TX node，不是常量或invalid sentinel；
- FIB prerequisite gate全通过后才存在interface route call site；
- admin-down interface可保存ordinary addresses但没有interface-source ordinary routes；
- admin up/down与address add/delete从同一private family helpers得到完全相同的route集合；
- local、connected、IPv4 special与IPv6 link-local contributions都进入真实FIB并拥有正确DPO；
- 同interface同prefix多个addresses正确共享prefix refcount，last delete才撤销共享route；
- interface delete不会double-remove routes或遗留DPO/FIB references；
- public address error仍只有ADR-0028六类；FIB与interface route mutation不新增`FibError`或route
  errno，missing-prefix no-op、missing-source无error/missing boolean及prefix-record warning分支和
  VPP一致；
- 没有`InterfaceRxDpo`冒充、直接trie写入、IP state下沉service或新family facade。

## 13. Decision record

| ID | Final decision | Alignment |
| --- | --- | --- |
| D1 | 独立generic software admin callback inventory；IP4/IP6 route和glean callbacks注册LOW，neighbor注册HIGH，稳定排序保证所有LOW先于HIGH | 对齐V8-V9,V38 ownership、事件类别和priority |
| D2 | 每次ordinary address add/delete都读取当前admin flag并决定是否立即add/del routes；admin transition另行遍历全部已有addresses；两个入口调用同一family helpers | 对齐V1-V2,V8 |
| D3 | `IpLookupMain`拥有per-interface prefix pool/hash/refcount | 对齐V4,V6,V15 |
| D4 | IPv4实现local、connected attached resolver、network/broadcast default-drop和`/31`peer attached；P2P含tuntap走zero-neighbor，ordinary interface走glean/peer neighbor | 对齐V3-V5,V13,V32 |
| D5 | IPv6 ordinary实现local与connected attached resolver，P2P/ordinary分别走zero-neighbor/glean，并保留family-specific delete顺序 | 对齐V6-V7,V32 |
| D6 | link-local route由`Ip6LinkMain`的per-interface table拥有 | 对齐V14，不混入ordinary table |
| D7 | 先完成ADR-0005既有generic `FibEntrySrc`/`FibPathList`/`FibPath` live graph和VPP operation语义，删除当前直接DPO的平行source模型；FIB gate未通过不得接interface routes | 对齐V16-V21，复用已有FIB抽象而非新增interface-route FIB |
| D8 | `hammer-service::net::fib`只拥有泛型图、通用path comparator和生命周期；`Ip4FibBackend`/`Ip6FibBackend`分别拥有family path fixup、解析、LB bucket projection与forwarding store | 对齐V20,V35,V41的common FIB与protocol implementation边界，并由Rust单态化零成本复用 |
| D9 | interface helper只向对齐后的FIB提交path/special facts，不创建DPO/LB/uRPF或直接写trie | 对齐V3-V7,V10-V12,V17,V22 |
| D10 | public address error不扩张；FIB add/update返回entry index，remove/delete返回`()`；missing prefix删除no-op，missing source不产生error/missing boolean，interface-prefix record缺失warning+return；不增加`FibError`、rollback或panic语义 | 对齐VPP FIB与interface route helper的实际返回和错误形状 |
| D11 | admin callback按LOW再HIGH顺序执行，第一个error停止且只恢复flags；不增加reverse callback；两个IP callback不返回route error，后续callback失败也不撤销其route mutation | 对齐V9,V38的priority、错误与非事务语义 |
| D12 | service net拥有12-byte `FibNode`、heterogeneous `FibNodeList` pools和可注册`FibNodeOperations`；plugin注册get/last-lock/back-walk/memory静态operations并保留自己的typed pool/state | 对齐V25；允许插件实现concrete node，无`dyn`或plugin object下沉 |
| D13 | generic `FibMain<P,B,S>`按family和plugin-owned source state单态化并拥有该family全部table共享的entry/path-list/path pools；live path加入adjacency child list并保存所属path-list，active entry以parent/sibling加入winner path-list，形成完整back-walk链 | 对齐V25-V26,V34，消除per-table index冲突和value-only path |
| D14 | IP插件私有`IpFibPrefix`和单一4-cache-line `IpAdjacency` pool；embedded `FibNode`、完整rewrite、真实node index、DB key和DPO projection均按VPP源码固定；`rewrite.next_index`复用`DpoMain::stack_from_node`连接interface TX DPO | 对齐V24-V27,V39，撤回service `AdjacencyDpo<A,R>` |
| D15 | IPv6-ND FIB source由`Ip6LinkMain`向generic source registry注册并私有保存identity；service不定义plugin source | 对齐V16并保持plugin source ownership |
| D16 | service定义`FibSourceMain`、泛型`FibEntrySrc<S,...>`和`FibEntrySourceOperations<P,B,S>`，不定义public behavior trait；operations可跨crate构造且callback只接收owner/entry-index/source；IP插件定义`S=IpFibEntrySource`并在IP4/IP6 Main初始化时显式注册SIMPLE/API/INTERFACE/ADJACENCY operations；entry直接拥有sorted records | 对齐V16,V28-V29,V33,V36-V37；VFT value被真实写入behavior slot，删除封闭behavior match、按behavior source-record pools和跨callback内部borrow |
| D17 | `IpAdjacency`显式保留48-byte subtype union、delegates pointer槽与第4 cache-line offsets；link使用独立1-byte类型；glean/neighbor DB严格按VPP分区与key | 对齐V24,V27,V30，而非只对齐总size |
| D18 | service只增加读取现有`HwClassFlags::P2P`的`InterfaceMain::is_p2p`；attached resolver对P2P使用zero-next-hop neighbor，ordinary interface使用glean/peer neighbor；没有NBMA owner前不新增NBMA state | 对齐V32并复用现有interface class事实 |
| D19 | `FibSource::ADJACENCY`固定注册priority `0xd0`/ADJACENCY behavior；ADJ FES拥有独立source-state variant及per-path refinement extension，按cover attached/interface匹配决定neighbor host route是否安装；glean仍由INTERFACE source path解析产生 | 对齐V33；不把adjacency DPO、glean和neighbor FIB source混成一个生命周期 |
| D20 | 每个installed entry拥有稳定LB；winner/path/uRPF变化原地更新同一DPO identity，只有uninstall从trie移除并reset | 对齐V35，撤回projection replacement root模型 |
| D21 | path-list不含`source_count`；source lock和entry child统一使用embedded `FibNode.lock_count`，last lock执行唯一销毁路径 | 对齐V40，避免双重引用事实 |
| D22 | INTERFACE FES只注册VPP实际slots；ADJ cover-change和cover-update分别实现不同install/reason语义 | 对齐V37，不以统一callback结果代替源码语义 |
| D23 | service拥有internal `FibWalk` pool及HIGH/LOW queues；async walk以parent child sibling持锁，`FORCE_SYNC`改走同步，完成/取消对称解除queue与parent关系 | 对齐V34,V42，popular path-list不降级为无生命周期保护的index queue |

**Design verdict: Needs implementation.** ADR-0028 的address API只有在本文route与admin lifecycle
完成后才能宣称与vendored VPP的ordinary和link-local interface-address成功路径对齐。
