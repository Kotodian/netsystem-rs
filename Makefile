.PHONY: ios-lib xcframework clean-ios-lib

ios-lib:
	./scripts/build-xcframework.sh

xcframework: ios-lib

clean-ios-lib:
	rm -rf dist/ios
