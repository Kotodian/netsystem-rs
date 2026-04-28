import Hammer

/// Minimal platform implementation for the M1 macOS smoke test. Only `writeLog`
/// does any meaningful work — every other callback returns a benign value
/// because the real data path (TUN, network monitor, WiFi) lands in M5/M8.
final class StubPlatform: Platform {
    func openTun(options: TunOptions) throws -> Int32 {
        throw HammerError.Internal(message: "openTun is not available in M1 stub")
    }

    func usePlatformAutoDetectInterfaceControl() -> Bool { false }

    func autoDetectInterfaceControl(fd: Int32) throws { }

    func startDefaultInterfaceMonitor(listener: DefaultInterfaceUpdateListener) throws { }

    func closeDefaultInterfaceMonitor(listener: DefaultInterfaceUpdateListener) throws { }

    func getInterfaces() throws -> [NetworkInterface] { [] }

    func underNetworkExtension() -> Bool { false }

    func includeAllNetworks() -> Bool { false }

    func readWifiState() -> WifiState? { nil }

    func systemCertificates() -> [String] { [] }

    func clearDnsCache() { }

    func writeLog(level: Int32, message: String) {
        // Lines arrive newline-terminated from Rust.
        print(message, terminator: "")
    }
}
