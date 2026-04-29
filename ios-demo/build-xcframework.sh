#!/usr/bin/env bash
# Build an xcframework + Swift bindings for the hammer crate (iOS device only).
#
# We ship a dynamic framework (cdylib wrapped as `.framework`) instead of a
# static library so the cargo link step performs `-dead_strip`. That collapses
# the ~50 MB unstripped staticlib down to ~3 MB, comparable to gomobile output.
#
# Output layout:
#   build/Hammer.xcframework/ios-arm64/hammerFFI.framework
#   build/Sources/Hammer/hammer.swift
#
# Required toolchain:
#   - Xcode 15+
#   - Rust target: aarch64-apple-ios   (`rustup target add aarch64-apple-ios`)

set -euo pipefail

cd "$(dirname "$0")/.."

# Match the deployment target wired into the C deps (aws-lc-sys, ring) so the
# linker doesn't trip on `___chkstk_darwin` (introduced in iOS 13).
export IPHONEOS_DEPLOYMENT_TARGET="${IPHONEOS_DEPLOYMENT_TARGET:-15.0}"

BUILD_DIR="ios-demo/build"
GENERATED_DIR="${BUILD_DIR}/generated"
XCFRAMEWORK="${BUILD_DIR}/Hammer.xcframework"
FRAMEWORK="${BUILD_DIR}/ios/Hammer.framework"
DYLIB_SRC="target/aarch64-apple-ios/release/libhammer.dylib"

rm -rf "${BUILD_DIR}"
mkdir -p "${GENERATED_DIR}" "${FRAMEWORK}/Headers" "${FRAMEWORK}/Modules"

echo "==> cargo build cdylib (release, aarch64-apple-ios)"
cargo build -p hammer-ffi --release --target aarch64-apple-ios

echo "==> assemble hammerFFI.framework"
cp "${DYLIB_SRC}" "${FRAMEWORK}/Hammer"
strip -S -x "${FRAMEWORK}/Hammer"
# Frameworks must use an @rpath install_name so the host app can embed them.
install_name_tool -id @rpath/Hammer.framework/Hammer "${FRAMEWORK}/Hammer"

echo "==> generate Swift bindings via uniffi-bindgen"
cargo run -p hammer-uniffi-bindgen --release -- \
  generate crates/hammer-ffi/src/hammer.udl \
  --language swift --out-dir "${GENERATED_DIR}"
cp "${GENERATED_DIR}/hammerFFI.h" "${FRAMEWORK}/Headers/Hammer.h"

# `framework module` (not plain `module`) is required for `import Hammer`
# to resolve against a binary framework target.
cat > "${FRAMEWORK}/Modules/module.modulemap" <<'EOM'
framework module Hammer {
    header "Hammer.h"
    export *
}
EOM

cat > "${FRAMEWORK}/Info.plist" <<'EOM'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleIdentifier</key><string>com.kotodian.Hammer</string>
  <key>CFBundleName</key><string>Hammer</string>
  <key>CFBundleExecutable</key><string>Hammer</string>
  <key>CFBundleVersion</key><string>1</string>
  <key>CFBundleShortVersionString</key><string>0.1.0</string>
  <key>CFBundlePackageType</key><string>FMWK</string>
  <key>MinimumOSVersion</key><string>15.0</string>
  <key>CFBundleSupportedPlatforms</key><array><string>iPhoneOS</string></array>
</dict>
</plist>
EOM

echo "==> assemble xcframework"
xcodebuild -create-xcframework \
  -framework "${FRAMEWORK}" \
  -output "${XCFRAMEWORK}"

echo "==> publish Swift glue"
mkdir -p "${BUILD_DIR}/Sources/Hammer"
# uniffi emits `import hammerFFI` based on the udl namespace; rewrite to match
# the framework module name we ship (Hammer).
sed -i.bak 's/import hammerFFI/import Hammer/g' "${GENERATED_DIR}/hammer.swift"
rm "${GENERATED_DIR}/hammer.swift.bak"
mv "${GENERATED_DIR}/hammer.swift" "${BUILD_DIR}/Sources/Hammer/hammer.swift"

echo
echo "Done."
du -sh "${XCFRAMEWORK}"
echo "  XCFramework: ${XCFRAMEWORK}"
echo "  Swift glue:  ${BUILD_DIR}/Sources/Hammer/hammer.swift"
