# pgswift

![Swift 5.0](https://img.shields.io/badge/Swift-5.0-orange)  ![License: ISC](https://img.shields.io/github/license/mlilback/pgswift) ![platforms: macOS  | Linux](https://img.shields.io/badge/platform-macOS%20%7C%20Linux-lightgrey) 

A PostgreSQL driver for use in swift. Requires Swift 5.0+. Builds with Swift Package Manager. It depends on libpq being available, but the package manager knows how install that vai brew/apt-get. 

Every other driver I've seen requires as associated ORM system. I just want raw calls to postgresql.

This is being actively developed. Not even close to a release, though it has enough basic features to use in a [Kitura](https://kitura.io/) app I'm building.

The one feature that sets it apart from the other libraries (to me) is support for notifications (LISTEN/NOTIFY) using a DispatchQueue &mdash; the only other implementation I've seen uses polling!

## Testing

XCTest is used, but they are integration/system tests, not unit tests. Since all of the code deals with a remote database, writing mocks would insanely difficult. There is a shell script that starts up a docker container. The test will fatal error if the pgtest container is not available.

The tests are very comprehensive. Most code not covered is error handling from the database or network that are not reproducible. 

## Continuous Integration

CircleCI doesn't support macOS. Travis doesn't support using docker containers on macOS. That means no CI.

## Documentation

[API documentation](https://mlilback.github.io/pgswift) is enerated using [jazzy](https://github.com/realm/jazzy). It uses Ruby, so I'd highly recommend using [rbenv](https://github.com/rbenv/rbenv) instead of the system Ruby since the syystem Ruby is old and deprectated in Catalina.  And you'd have to use sudo to install it in system directories.

