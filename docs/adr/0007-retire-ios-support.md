# Retire iOS support

Hammer will retire the old iOS NetworkExtension and xcframework support path instead of preserving it behind adapter or FFI compatibility layers. The project is now a standalone VPP-style data-plane framework with daemon, CLI, runtime, service, app, IPC, core, and infra crates; future work should not keep iOS-specific build targets, FFI abstractions, packaging scripts, generated framework output, `dist/ios` conventions, or documentation as supported surfaces.
