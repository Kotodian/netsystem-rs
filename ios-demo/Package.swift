// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "HammerDemo",
    platforms: [.macOS(.v13)],
    products: [
        .executable(name: "HammerDemo", targets: ["HammerDemo"]),
    ],
    targets: [
        .binaryTarget(
            name: "HammerXCFramework",
            path: "build/Hammer.xcframework"
        ),
        .target(
            name: "Hammer",
            dependencies: ["HammerXCFramework"],
            path: "build/Sources/Hammer"
        ),
        .executableTarget(
            name: "HammerDemo",
            dependencies: ["Hammer"],
            path: "Sources/HammerDemo"
        ),
    ]
)
