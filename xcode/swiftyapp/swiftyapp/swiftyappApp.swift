//
//  swiftyappApp.swift
//  swiftyapp
//
//  Created by Jonathan McKenzie on 7/9/24.
//

import SwiftUI
import os.log

@main
struct swiftyappApp: App {
    init() {
        let logger = Logger(subsystem: "org.gnostr.xcode-rbitcoin", category: "launch")
        logger.info("App launch begin")
        do {
            let version = rbitcoinVersion()
            logger.info("Rust FFI loaded: version=\(version)")
            let hello = rustHello()
            logger.info("Rust hello: \(hello)")
        } catch {
            logger.error("Rust FFI failed: \(error.localizedDescription)")
        }
        logger.info("App launch end")
    }

    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}
