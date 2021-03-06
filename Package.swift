// swift-tools-version:5.3
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
	name: "pgswift",
	platforms: [
		.macOS(.v11)
	],
	products: [
		// Products define the executables and libraries produced by a package, and make them visible to other packages.
		.library(
			name: "pgswift",
			targets: ["pgswift"]),
	],
	dependencies: [
		.package(url: "https://github.com/apple/swift-log.git", from: "1.1.1"),
	],
	targets: [
		// Targets are the basic building blocks of a package. A target can define a module or a test suite.
		// Targets can depend on other targets in this package, and on products in packages which this package depends on.
		.systemLibrary(
			name: "CLibpq",
			pkgConfig: "libpq",
			providers: [
				.brew(["postgresql"]),
				.apt(["libpq-dev"])
			]
		),
		.target(
			name: "pgswift",
			dependencies: [
			"CLibpq", 
			.product(name: "Logging", package: "swift-log")
			]),
		.testTarget(
			name: "pgswiftTests",
			dependencies: ["pgswift"]),
	]
)
