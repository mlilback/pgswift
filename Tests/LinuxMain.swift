import XCTest

import pgswiftTests

var tests = [XCTestCaseEntry]()
tests += pgswiftTests.allTests()
tests += QueryTests.allTests()
XCTMain(tests)
