import Foundation
import Hammer

let toml = """
[log]
level = "debug"

[tun]
mtu = 9000
stack = "system"
address = ["172.19.0.1/30"]
route_address = ["0.0.0.0/0"]

[hysteria2]
server = "example.com"
password = "demo"
sni = "example.com"

[dns]
server = "https://1.1.1.1/dns-query"

[route]
final = "hysteria2"
"""

do {
    try checkConfig(content: toml)
    print(">>> checkConfig ok")

    let svc = try newService(configContent: toml, platform: StubPlatform())
    print(">>> newService ok")

    try svc.start()
    print(">>> service.start ok")

    Thread.sleep(forTimeInterval: 0.5)

    try svc.close()
    print(">>> service.close ok")
} catch let HammerError.ConfigValidation(message) {
    fputs("config validation failed: \(message)\n", stderr)
    exit(1)
} catch let HammerError.ConfigParse(message) {
    fputs("config parse failed: \(message)\n", stderr)
    exit(2)
} catch {
    fputs("unexpected error: \(error)\n", stderr)
    exit(3)
}
