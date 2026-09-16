//
//  ContentView.swift
//  swiftyapp
//
//  Created by Jonathan McKenzie on 7/9/24.
//

import SwiftUI

struct ContentView: View {
    @Environment(\.colorScheme) private var colorScheme
    @State private var firstValue = 10
    @State private var secondValue = 32
    @State private var addressInput = "bc1qxy2kgdygjrsqtzq2n0yrf2493p83kkfjhx0wlh"
    @State private var addressNetworkResult = ""
    @State private var hashInput = "Hello rbitcoin"
    @State private var hashResult = ""
    @State private var blockHexInput = ""
    @State private var blockValidationResult = ""
    @State private var subsidyHeight = "840000"
    @State private var subsidyNetwork = "mainnet"
    @State private var subsidyResult = ""
    @State private var storePath = ""
    @State private var storeTipHeight = ""
    @State private var storeHeaderCount = ""
    @State private var queryPath = ""
    @State private var queryBlockQueue = ""
    @State private var mempoolPath = ""
    @State private var mempoolLiveCount = ""
    @State private var mempoolSlotStats = ""
    @State private var feeTargetBlocks = "1"
    @State private var feeStockAbove = "0"
    @State private var feeResult = ""
    @State private var txHexInput = ""
    @State private var txParseResult = ""
    @State private var headerHexInput = ""
    @State private var headerHashResult = ""
    @State private var networkSeedResult = ""
    @State private var scriptHashInput = ""
    @State private var scriptHashResult = ""
    @State private var logLevelInput = "info"
    @State private var logLevelResult = ""
    @State private var capturedLogs = ""
    @State private var genesisNetwork = "mainnet"
    @State private var genesisHashResult = ""
    @State private var milestoneResult = ""
    @State private var resolvedSeedsResult = ""
    @State private var constantsResult = ""
    @State private var regtestPrevHash = "0000000000000000000000000000000000000000000000000000000000000000"
    @State private var regtestTime = "1296688602"
    @State private var regtestHeight = "0"
    @State private var regtestBlockResult = ""
    @State private var serviceFlagsResult = ""
    @State private var versionbitsResult = ""
    @State private var merkleTxids = ""
    @State private var merkleRootResult = ""
    @State private var queryExtendedResult = ""
    @State private var headerHashVersion = "1"
    @State private var headerHashPrev = "0000000000000000000000000000000000000000000000000000000000000000"
    @State private var headerHashMerkle = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
    @State private var headerHashTime = "1231006505"
    @State private var headerHashBits = "486604799"
    @State private var headerHashNonce = "2083236893"
    @State private var computedHeaderHashResult = ""
    @State private var feeAtRate = "100"
    @State private var feeAtResult = ""
    @State private var archiveHashInput = ""
    @State private var archiveResult = ""
    @State private var witnessWtxids = ""
    @State private var witnessReserved = "0000000000000000000000000000000000000000000000000000000000000000"
    @State private var witnessScriptResult = ""
    @State private var sigopsTxHex = ""
    @State private var sigopsResult = ""
    @State private var bip68TxHex = ""
    @State private var bip68Result = ""
    @State private var libreTxHex = ""
    @State private var libreFee = "1000"
    @State private var libreWeight = "1000"
    @State private var libreResult = ""
    @State private var regtestPayScript = "76a914000000000000000000000000000000000000000088ac"
    @State private var regtestPayResult = ""
    @State private var seedServicesResult = ""
    @State private var queryFkTxid = ""
    @State private var queryFkResult = ""
    @State private var querySpentResult = ""
    @State private var queryWarningsResult = ""
    @State private var spTxHex = ""
    @State private var spPrevoutsHex = ""
    @State private var spResult = ""
    @State private var asmapHex = ""
    @State private var asmapIp = "1.2.0.0"
    @State private var asmapResult = ""
    @State private var queryDrainResult = ""
    @State private var queryShResult = ""
    @State private var blockStructHex = ""
    @State private var blockStructResult = ""
    @State private var netgroupIp = "1.2.3.4"
    @State private var netgroupPort = "8333"
    @State private var netgroupResult = ""
    @State private var workHexes = ""
    @State private var workResult = ""
    @State private var tweaksHeight = "0"
    @State private var tweaksResult = ""

    private var sum: Int {
        Int(rustAdd(a: UInt32(firstValue), b: UInt32(secondValue)))
    }

    var body: some View {
        ZStack {
            LinearGradient(
                colors: [
                    colorScheme == .dark
                        ? Color(red: 0.05, green: 0.05, blue: 0.07)
                        : Color(red: 0.95, green: 0.96, blue: 0.98),
                    colorScheme == .dark
                        ? Color(red: 0.10, green: 0.08, blue: 0.06)
                        : Color(red: 0.90, green: 0.92, blue: 0.96),
                    colorScheme == .dark
                        ? Color(red: 0.18, green: 0.09, blue: 0.03)
                        : Color(red: 0.82, green: 0.86, blue: 0.93)
                ],
                startPoint: .topLeading,
                endPoint: .bottomTrailing
            )
            .ignoresSafeArea()

            if colorScheme == .dark {
                RadialGradient(
                    colors: [
                        Color(red: 1.0, green: 0.60, blue: 0.15).opacity(0.20),
                        .clear
                    ],
                    center: .topTrailing,
                    startRadius: 20,
                    endRadius: 340
                )
                .ignoresSafeArea()
            } else {
                RadialGradient(
                    colors: [
                        Color(red: 0.77, green: 0.86, blue: 1.0).opacity(0.34),
                        .clear
                    ],
                    center: .topTrailing,
                    startRadius: 24,
                    endRadius: 360
                )
                .ignoresSafeArea()
            }

            ScrollView {
                VStack(alignment: .leading, spacing: 18) {
                    header

                    glassCard {
                        HStack(alignment: .center, spacing: 16) {
                            Image(colorScheme == .dark ? "RustOrb" : "RustOrbLight")
                                .resizable()
                                .scaledToFit()
                                .frame(width: 84, height: 84)
                                .padding(8)
                                .background(
                                    colorScheme == .dark
                                        ? .white.opacity(0.04)
                                        : .white.opacity(0.70),
                                    in: RoundedRectangle(cornerRadius: 22, style: .continuous)
                                )
                                .overlay(
                                    RoundedRectangle(cornerRadius: 22, style: .continuous)
                                        .stroke(
                                            colorScheme == .dark
                                                ? Color(red: 1.0, green: 0.66, blue: 0.24).opacity(0.28)
                                                : Color(red: 0.52, green: 0.62, blue: 0.72).opacity(0.22),
                                            lineWidth: 1
                                        )
                                )

                            VStack(alignment: .leading, spacing: 10) {
                                Label("Rust bridge", systemImage: "sparkles")
                                    .font(.headline)
                                    .foregroundStyle(accentText)

                                Text(rustHello())
                                    .font(.title2.weight(.semibold))
                                    .foregroundStyle(primaryText)

                                Text("SwiftUI talking to Rust, dressed up in the same warm palette as the icon.")
                                    .font(.subheadline)
                                    .foregroundStyle(primaryText.opacity(0.74))
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Live calculator", systemImage: "function")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("Rust powered")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            stepperRow(
                                title: "First value",
                                value: $firstValue,
                                range: 0...100
                            )

                            stepperRow(
                                title: "Second value",
                                value: $secondValue,
                                range: 0...100
                            )

                            Divider()
                                .overlay(accentFill.opacity(colorScheme == .dark ? 0.22 : 0.16))

                            HStack(alignment: .firstTextBaseline) {
                                VStack(alignment: .leading, spacing: 4) {
                                    Text("Result")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText.opacity(0.68))
                                    Text("\(firstValue) + \(secondValue)")
                                        .font(.title3.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }

                                Spacer()

                                Text("\(sum)")
                                    .font(.system(size: 42, weight: .bold, design: .rounded))
                                    .foregroundStyle(
                                        LinearGradient(
                                            colors: colorScheme == .dark
                                                ? [.white, Color(red: 1.0, green: 0.76, blue: 0.42)]
                                                : [Color(red: 0.10, green: 0.16, blue: 0.24), Color(red: 0.38, green: 0.45, blue: 0.58)],
                                            startPoint: .top,
                                            endPoint: .bottom
                                        )
                                    )
                            }

                            Button {
                                firstValue = Int.random(in: 0...100)
                                secondValue = Int.random(in: 0...100)
                            } label: {
                                Label("Randomize values", systemImage: "dice.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("rbitcoin primitives", systemImage: "bitcoinsign.circle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text(rbitcoinVersion())
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Bitcoin address")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                TextField("Enter address", text: $addressInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.body, design: .monospaced))
                                    .onChange(of: addressInput) { _ in updateAddressResult() }
                            }

                            HStack(spacing: 12) {
                                HStack(spacing: 6) {
                                    Image(systemName: validateAddress(address: addressInput) ? "checkmark.circle.fill" : "xmark.circle.fill")
                                        .foregroundStyle(validateAddress(address: addressInput) ? Color.green : Color.red)
                                    Text(validateAddress(address: addressInput) ? "Valid" : "Invalid")
                                        .font(.caption.weight(.semibold))
                                }
                                if !addressNetworkResult.isEmpty {
                                    Text(addressNetworkResult)
                                        .font(.caption.weight(.semibold))
                                        .padding(.horizontal, 10)
                                        .padding(.vertical, 4)
                                        .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                        .foregroundStyle(accentText)
                                }
                            }

                            Divider()
                                .overlay(accentFill.opacity(colorScheme == .dark ? 0.22 : 0.16))

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Hash256")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                TextField("Enter text to hash", text: $hashInput)
                                    .textFieldStyle(.roundedBorder)
                                    .onChange(of: hashInput) { _ in updateHashResult() }
                                if !hashResult.isEmpty {
                                    Text(hashResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }
                    .onAppear {
                        updateAddressResult()
                        updateHashResult()
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("rbitcoin consensus", systemImage: "shield.checkered")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("stateless")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Block subsidy")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                HStack(spacing: 8) {
                                    TextField("Height", text: $subsidyHeight)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 100)
                                    Picker("Network", selection: $subsidyNetwork) {
                                        Text("mainnet").tag("mainnet")
                                        Text("testnet").tag("testnet")
                                        Text("regtest").tag("regtest")
                                        Text("signet").tag("signet")
                                    }
                                    .pickerStyle(.menu)
                                    .tint(accentText)
                                    Spacer()
                                }
                                .onChange(of: subsidyHeight) { _ in updateSubsidy() }
                                .onChange(of: subsidyNetwork) { _ in updateSubsidy() }
                                if !subsidyResult.isEmpty {
                                    Text(subsidyResult)
                                        .font(.title3.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }

                            Divider()
                                .overlay(accentFill.opacity(colorScheme == .dark ? 0.22 : 0.16))

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Block wire validation")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                TextField("Paste block hex", text: $blockHexInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                    .onChange(of: blockHexInput) { _ in updateBlockValidation() }
                                if !blockValidationResult.isEmpty {
                                    HStack(spacing: 6) {
                                        Image(systemName: blockValidationResult == "Valid" ? "checkmark.circle.fill" : "xmark.circle.fill")
                                            .foregroundStyle(blockValidationResult == "Valid" ? Color.green : Color.red)
                                        Text(blockValidationResult)
                                            .font(.caption.weight(.semibold))
                                    }
                                }
                            }
                        }
                    }
                    .onAppear {
                        updateSubsidy()
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("rbitcoin store", systemImage: "externaldrive.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("embedded")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Store path")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                TextField("Documents subdirectory", text: $storePath)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.body, design: .monospaced))
                                    .onAppear {
                                        let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
                                        storePath = docs.appendingPathComponent("rbitcoin-store").path
                                    }
                            }

                            HStack(spacing: 12) {
                                Button {
                                    do {
                                        let store = try FfiStore.create(path: storePath)
                                        storeTipHeight = store.tipHeight().map(String.init) ?? "none"
                                        storeHeaderCount = String(store.headerCount())
                                    } catch {
                                        storeTipHeight = "error"
                                        storeHeaderCount = ""
                                    }
                                } label: {
                                    Label("Create", systemImage: "plus.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())

                                Button {
                                    do {
                                        let store = try FfiStore.open(path: storePath)
                                        storeTipHeight = store.tipHeight().map(String.init) ?? "none"
                                        storeHeaderCount = String(store.headerCount())
                                    } catch {
                                        storeTipHeight = "error"
                                        storeHeaderCount = ""
                                    }
                                } label: {
                                    Label("Open", systemImage: "folder.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                            }

                            if !storeTipHeight.isEmpty {
                                HStack(spacing: 16) {
                                    VStack(alignment: .leading, spacing: 4) {
                                        Text("Tip height")
                                            .font(.caption.weight(.semibold))
                                            .foregroundStyle(primaryText.opacity(0.68))
                                        Text(storeTipHeight)
                                            .font(.title3.weight(.semibold))
                                            .foregroundStyle(primaryText)
                                    }
                                    VStack(alignment: .leading, spacing: 4) {
                                        Text("Headers")
                                            .font(.caption.weight(.semibold))
                                            .foregroundStyle(primaryText.opacity(0.68))
                                        Text(storeHeaderCount)
                                            .font(.title3.weight(.semibold))
                                            .foregroundStyle(primaryText)
                                    }
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("rbitcoin query", systemImage: "magnifyingglass.circle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("chain view")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Query path")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                TextField("Documents subdirectory", text: $queryPath)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.body, design: .monospaced))
                                    .onAppear {
                                        let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
                                        queryPath = docs.appendingPathComponent("rbitcoin-query").path
                                    }
                            }

                            Button {
                                do {
                                    let query = try FfiQuery.openOrCreate(path: queryPath)
                                    let count = query.blockQueueCount()
                                    let maxH = query.blockQueueMaxHeight().map(String.init) ?? "none"
                                    queryBlockQueue = "\(count) blocks, max height: \(maxH)"
                                } catch {
                                    queryBlockQueue = "error"
                                }
                            } label: {
                                Label("Open or create query", systemImage: "arrow.up.doc.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !queryBlockQueue.isEmpty {
                                Text(queryBlockQueue)
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("rbitcoin mempool", systemImage: "memorychip.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("unconfirmed")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Mempool path")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                TextField("Documents subdirectory", text: $mempoolPath)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.body, design: .monospaced))
                                    .onAppear {
                                        let docs = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).first!
                                        mempoolPath = docs.appendingPathComponent("rbitcoin-mempool").path
                                    }
                            }

                            Button {
                                do {
                                    let mempool = try FfiMempool.openOrCreate(path: mempoolPath)
                                    let count = mempool.liveCount()
                                    let stats = mempool.slotStats()
                                    mempoolLiveCount = "\(count) live"
                                    mempoolSlotStats = "free: \(stats.free), live: \(stats.live), dead: \(stats.dead)"
                                } catch {
                                    mempoolLiveCount = "error"
                                    mempoolSlotStats = ""
                                }
                            } label: {
                                Label("Open or create mempool", systemImage: "arrow.up.doc.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !mempoolLiveCount.isEmpty {
                                VStack(alignment: .leading, spacing: 4) {
                                    Text(mempoolLiveCount)
                                        .font(.title3.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                    Text(mempoolSlotStats)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText.opacity(0.68))
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Fee estimation", systemImage: "chart.line.uptrend.xyaxis")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-mempool")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Target blocks")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                HStack(spacing: 8) {
                                    TextField("Blocks", text: $feeTargetBlocks)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                    Text("stock above (WU)")
                                        .font(.caption)
                                        .foregroundStyle(primaryText.opacity(0.68))
                                    TextField("WU", text: $feeStockAbove)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 100)
                                    Spacer()
                                }
                                .onChange(of: feeTargetBlocks) { _ in updateFeeResult() }
                                .onChange(of: feeStockAbove) { _ in updateFeeResult() }
                                if !feeResult.isEmpty {
                                    Text(feeResult)
                                        .font(.title3.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }
                    .onAppear {
                        updateFeeResult()
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Transaction parser", systemImage: "doc.text.magnifyingglass")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("bitcoin crate")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Transaction hex")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                TextField("Paste tx hex", text: $txHexInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                    .onChange(of: txHexInput) { _ in updateTxParse() }
                                if !txParseResult.isEmpty {
                                    Text(txParseResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Block header hash", systemImage: "cube.transparent")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("bitcoin crate")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Header hex")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                TextField("Paste header hex", text: $headerHexInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                    .onChange(of: headerHexInput) { _ in updateHeaderHash() }
                                if !headerHashResult.isEmpty {
                                    Text(headerHashResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("P2P network", systemImage: "network")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                do {
                                    let seeds = try p2pDnsSeeds(network: "mainnet")
                                    let port = try p2pDefaultPort(network: "mainnet")
                                    networkSeedResult = "Port: \(port), Seeds: \(seeds.count)"
                                } catch {
                                    networkSeedResult = "error"
                                }
                            } label: {
                                Label("Load mainnet seeds", systemImage: "arrow.down.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !networkSeedResult.isEmpty {
                                Text(networkSeedResult)
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Electrum scripthash", systemImage: "number")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-electrum")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Script hex")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                TextField("Paste script hex", text: $scriptHashInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                    .onChange(of: scriptHashInput) { _ in updateScriptHash() }
                                if !scriptHashResult.isEmpty {
                                    Text(scriptHashResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("rbitcoin log", systemImage: "doc.text.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-log")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                Text("Log level")
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.92))
                                HStack(spacing: 8) {
                                    TextField("Level", text: $logLevelInput)
                                        .textFieldStyle(.roundedBorder)
                                        .frame(width: 100)
                                    Button {
                                        initLogLevel(level: logLevelInput)
                                        logLevelResult = logLevelEnabled(level: logLevelInput) ? "enabled" : "disabled"
                                    } label: {
                                        Label("Set", systemImage: "slider.horizontal.3")
                                    }
                                    .buttonStyle(PrimaryButtonStyle())
                                    Spacer()
                                }
                                if !logLevelResult.isEmpty {
                                    Text(logLevelResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }

                            HStack(spacing: 12) {
                                Button {
                                    captureLogs(on: true)
                                    logMessage(level: "info", message: "SwiftUI capture test")
                                    let logs = takeLogs()
                                    capturedLogs = logs.joined(separator: "\n")
                                    captureLogs(on: false)
                                } label: {
                                    Label("Capture", systemImage: "arrow.down.doc.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())

                                if !capturedLogs.isEmpty {
                                    Text(capturedLogs)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(2)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Genesis block", systemImage: "globe")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            HStack(spacing: 8) {
                                Picker("Network", selection: $genesisNetwork) {
                                    Text("mainnet").tag("mainnet")
                                    Text("testnet").tag("testnet")
                                    Text("regtest").tag("regtest")
                                    Text("signet").tag("signet")
                                }
                                .pickerStyle(.menu)
                                .tint(accentText)
                                Button {
                                    do {
                                        let hash = try genesisBlockHash(network: genesisNetwork)
                                        let milestone = try defaultMilestoneHeight(network: genesisNetwork)
                                        genesisHashResult = hash
                                        milestoneResult = "milestone: \(milestone)"
                                    } catch {
                                        genesisHashResult = "error"
                                        milestoneResult = ""
                                    }
                                } label: {
                                    Label("Load", systemImage: "arrow.down.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }

                            if !genesisHashResult.isEmpty {
                                Text(genesisHashResult)
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundStyle(primaryText.opacity(0.74))
                                    .lineLimit(1)
                                Text(milestoneResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.68))
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Seed resolution", systemImage: "network.badge.shield.half.filled")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                do {
                                    let fixed = try resolveFixedSeeds(network: "mainnet")
                                    let dns = try resolveDnsSeeds(network: "mainnet")
                                    let all = try resolveAllSeeds(network: "mainnet")
                                    resolvedSeedsResult = "fixed: \(fixed.count) dns: \(dns.count) all: \(all.count)"
                                } catch {
                                    resolvedSeedsResult = "error"
                                }
                            } label: {
                                Label("Resolve mainnet seeds", systemImage: "arrow.triangle.2.circlepath")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !resolvedSeedsResult.isEmpty {
                                Text(resolvedSeedsResult)
                                    .font(.subheadline.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Constants", systemImage: "number.circle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                let magic = rbitcoinStoreMagic()
                                let schema = rbitcoinSchemaVersion()
                                let maxWeight = mempoolDefaultMaxWeight()
                                let maxTxWeight = mempoolMaxStandardTxWeight()
                                let dust = electrumDefaultTweaksMinDust()
                                constantsResult = "magic=\(magic) schema=\(schema) maxWeight=\(maxWeight) maxTx=\(maxTxWeight) dust=\(dust)"
                            } label: {
                                Label("Load constants", systemImage: "info.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !constantsResult.isEmpty {
                                Text(constantsResult)
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundStyle(primaryText.opacity(0.74))
                                    .lineLimit(2)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Regtest mining", systemImage: "hammer.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                HStack(spacing: 8) {
                                    TextField("Prev hash", text: $regtestPrevHash)
                                        .textFieldStyle(.roundedBorder)
                                        .font(.system(.caption, design: .monospaced))
                                    TextField("Time", text: $regtestTime)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 100)
                                    TextField("Height", text: $regtestHeight)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 60)
                                }
                                Button {
                                    guard let time = UInt32(regtestTime), let height = UInt32(regtestHeight) else {
                                        regtestBlockResult = "invalid input"
                                        return
                                    }
                                    do {
                                        let hex = try mineEmptyRegtest(prevHashHex: regtestPrevHash, time: time, height: height)
                                        regtestBlockResult = "block: \(hex.prefix(32))…"
                                    } catch {
                                        regtestBlockResult = "error"
                                    }
                                } label: {
                                    Label("Mine block", systemImage: "hammer.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !regtestBlockResult.isEmpty {
                                    Text(regtestBlockResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Service flags", systemImage: "flag.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                let local = localServiceFlagsU64()
                                let desirable = desirableServiceFlags(offered: local, tipDepthBlocks: 0)
                                let hasAll = hasAllDesirableServiceFlags(offered: local, tipDepthBlocks: 0)
                                serviceFlagsResult = "local=0x\(String(local, radix: 16)) desirable=0x\(String(desirable, radix: 16)) hasAll=\(hasAll)"
                            } label: {
                                Label("Check service flags", systemImage: "antenna.radiowaves.left.and.right")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !serviceFlagsResult.isEmpty {
                                Text(serviceFlagsResult)
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundStyle(primaryText.opacity(0.74))
                                    .lineLimit(1)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Versionbits", systemImage: "exclamationmark.triangle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                do {
                                    let period = try warnPeriodThreshold(network: "mainnet")
                                    let warning = unknownRulesWarning(bit: 0)
                                    versionbitsResult = "period: \(period.start)-\(period.end) warn: \(warning.prefix(20))…"
                                } catch {
                                    versionbitsResult = "error"
                                }
                            } label: {
                                Label("Load warnings", systemImage: "bell.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !versionbitsResult.isEmpty {
                                Text(versionbitsResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Merkle root", systemImage: "tree.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-store")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Txids (comma-separated)", text: $merkleTxids)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let txids = merkleTxids.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
                                    do {
                                        let root = try merkleRootFromTxids(txidsHex: txids)
                                        merkleRootResult = root
                                    } catch {
                                        merkleRootResult = "error"
                                    }
                                } label: {
                                    Label("Compute root", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !merkleRootResult.isEmpty {
                                    Text(merkleRootResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Query stats", systemImage: "chart.bar.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-query")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                do {
                                    let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-query-demo")
                                    let window = query.softConfirmWindow()
                                    let archived = try query.archivedBlockCount()
                                    let txBody = query.txBodyCount()
                                    let txHead = query.txHeadOccupied()
                                    queryExtendedResult = "window=\(window) archived=\(archived) txBody=\(txBody) txHead=\(txHead)"
                                } catch {
                                    queryExtendedResult = "error"
                                }
                            } label: {
                                Label("Load query stats", systemImage: "arrow.up.doc.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !queryExtendedResult.isEmpty {
                                Text(queryExtendedResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Header hash", systemImage: "number")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-store")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                HStack(spacing: 8) {
                                    TextField("Ver", text: $headerHashVersion)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 50)
                                    TextField("Bits", text: $headerHashBits)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                    TextField("Nonce", text: $headerHashNonce)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                }
                                TextField("Prev hash", text: $headerHashPrev)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("Merkle root", text: $headerHashMerkle)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                HStack(spacing: 8) {
                                    TextField("Time", text: $headerHashTime)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 100)
                                    Button {
                                        guard let version = Int32(headerHashVersion), let time = UInt32(headerHashTime), let bits = UInt32(headerHashBits), let nonce = UInt32(headerHashNonce) else {
                                            computedHeaderHashResult = "invalid input"
                                            return
                                        }
                                        do {
                                            let hash = try blockHeaderHash(version: version, prevHashHex: headerHashPrev, merkleRootHex: headerHashMerkle, timestamp: time, bits: bits, nonce: nonce)
                                            computedHeaderHashResult = hash
                                        } catch {
                                            computedHeaderHashResult = "error"
                                        }
                                    } label: {
                                        Label("Hash", systemImage: "arrow.right.circle.fill")
                                    }
                                    .buttonStyle(PrimaryButtonStyle())
                                    Spacer()
                                }
                                if !computedHeaderHashResult.isEmpty {
                                    Text(computedHeaderHashResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Fee at rate", systemImage: "dollarsign.circle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            HStack(spacing: 8) {
                                TextField("Rate sat/kvB", text: $feeAtRate)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 100)
                                Button {
                                    guard let rate = UInt64(feeAtRate) else {
                                        feeAtResult = "invalid"
                                        return
                                    }
                                    let ok = meetsMinRelayFeeAt(feeSat: rate, weight: 4000, satKvb: rate)
                                    feeAtResult = ok ? "meets min relay" : "below min relay"
                                } label: {
                                    Label("Check", systemImage: "checkmark.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !feeAtResult.isEmpty {
                                Text(feeAtResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Archive probe", systemImage: "archivebox.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-query")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Block hash hex", text: $archiveHashInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    do {
                                        let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-archive-demo")
                                        let archived = try query.isBlockArchived(hashHex: archiveHashInput)
                                        archiveResult = archived ? "archived" : "not archived"
                                    } catch {
                                        archiveResult = "error"
                                    }
                                } label: {
                                    Label("Probe", systemImage: "magnifyingglass.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !archiveResult.isEmpty {
                                    Text(archiveResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Witness commitment", systemImage: "doc.plaintext.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Wtxids (comma-separated)", text: $witnessWtxids)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("Reserved", text: $witnessReserved)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let wtxids = witnessWtxids.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
                                    do {
                                        let script = try witnessCommitmentScript(nonCbWtxidsHex: wtxids, reservedHex: witnessReserved)
                                        witnessScriptResult = script
                                    } catch {
                                        witnessScriptResult = "error"
                                    }
                                } label: {
                                    Label("Build script", systemImage: "hammer.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !witnessScriptResult.isEmpty {
                                    Text(witnessScriptResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Sigops", systemImage: "function")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Tx hex", text: $sigopsTxHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = sigopsTxHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty else {
                                        sigopsResult = ""
                                        return
                                    }
                                    do {
                                        let legacy = try legacySigopCount(txHex: hex)
                                        let gbt = try txGbtSigops(txHex: hex)
                                        sigopsResult = "legacy: \(legacy) gbt: \(gbt)"
                                    } catch {
                                        sigopsResult = "invalid tx"
                                    }
                                } label: {
                                    Label("Count sigops", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !sigopsResult.isEmpty {
                                    Text(sigopsResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("BIP68 check", systemImage: "lock.shield.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Tx hex", text: $bip68TxHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = bip68TxHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty else {
                                        bip68Result = ""
                                        return
                                    }
                                    do {
                                        let active = try bip68ActiveForTx(txHex: hex)
                                        let locks = try sequenceLocksSatisfied(txHex: hex, prevHeights: [], prevCoinMtps: [], blockHeight: 100, blockPrevMtp: 100)
                                        bip68Result = active ? "BIP68 active, locks: \(locks)" : "BIP68 inactive"
                                    } catch {
                                        bip68Result = "invalid tx"
                                    }
                                } label: {
                                    Label("Check BIP68", systemImage: "checkmark.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !bip68Result.isEmpty {
                                    Text(bip68Result)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Libre policy", systemImage: "doc.badge.gearshape.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Tx hex", text: $libreTxHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                HStack(spacing: 8) {
                                    TextField("Fee", text: $libreFee)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                    TextField("Weight", text: $libreWeight)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                    Button {
                                        let hex = libreTxHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                        guard !hex.isEmpty, let fee = UInt64(libreFee), let weight = UInt64(libreWeight) else {
                                            libreResult = "invalid input"
                                            return
                                        }
                                        do {
                                            let annex = try checkLibreAnnex(txHex: hex)
                                            let admission = try checkLibreAdmission(txHex: hex, feeSat: fee, weight: weight)
                                            libreResult = "annex: \(annex), admit: \(admission)"
                                        } catch {
                                            libreResult = "invalid tx"
                                        }
                                    } label: {
                                        Label("Check", systemImage: "checkmark.circle.fill")
                                    }
                                    .buttonStyle(PrimaryButtonStyle())
                                    Spacer()
                                }
                                if !libreResult.isEmpty {
                                    Text(libreResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Regtest pay mining", systemImage: "hammer.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Script pubkey hex", text: $regtestPayScript)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    do {
                                        let hex = try mineRegtestPaying(prevHashHex: regtestPrevHash, time: 1296688602, height: 0, scriptPubkeyHex: regtestPayScript, extraTxsHex: [])
                                        regtestPayResult = "block: \(hex.prefix(32))…"
                                    } catch {
                                        regtestPayResult = "error"
                                    }
                                } label: {
                                    Label("Mine paying block", systemImage: "hammer.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !regtestPayResult.isEmpty {
                                    Text(regtestPayResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Seed services", systemImage: "antenna.radiowaves.left.and.right")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                let services = requiredSeedServicesU64()
                                seedServicesResult = "0x\(String(services, radix: 16))"
                            } label: {
                                Label("Load required services", systemImage: "info.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !seedServicesResult.isEmpty {
                                Text(seedServicesResult)
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundStyle(primaryText.opacity(0.74))
                                    .lineLimit(1)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Query tx lookup", systemImage: "magnifyingglass.circle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-query")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Txid hex", text: $queryFkTxid)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    do {
                                        let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-query-txfk")
                                        let fk = try query.txFkByTxid(txidHex: queryFkTxid)
                                        let spent = try query.isOutpointSpentAt(txidHex: queryFkTxid, vout: 0, tip: nil)
                                        queryFkResult = fk != nil ? "fk=\(fk!)" : "not found"
                                        querySpentResult = spent ? "spent" : "unspent"
                                    } catch {
                                        queryFkResult = "error"
                                        querySpentResult = ""
                                    }
                                } label: {
                                    Label("Lookup", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !queryFkResult.isEmpty {
                                    Text("\(queryFkResult), \(querySpentResult)")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Query warnings", systemImage: "exclamationmark.triangle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                do {
                                    let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-query-warn")
                                    let bits = try query.activeUnknownBits(network: "mainnet")
                                    let warnings = try query.warningStrings(network: "mainnet")
                                    queryWarningsResult = "bits: \(bits.count), warnings: \(warnings.count)"
                                } catch {
                                    queryWarningsResult = "error"
                                }
                            } label: {
                                Label("Check warnings", systemImage: "bell.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !queryWarningsResult.isEmpty {
                                Text(queryWarningsResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Silent payments", systemImage: "ear.badge.waveform")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Tx hex", text: $spTxHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("Prevouts hex (comma-separated)", text: $spPrevoutsHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = spTxHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty else {
                                        spResult = ""
                                        return
                                    }
                                    let prevouts = spPrevoutsHex.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
                                    do {
                                        let tweak = try tweakFromTx(txHex: hex, prevoutsHex: prevouts)
                                        if let t = tweak {
                                            spResult = "tweak: \(t.tweak.prefix(16))… outs: \(t.outputPubkeys.count)"
                                        } else {
                                            spResult = "not eligible"
                                        }
                                    } catch {
                                        spResult = "invalid input"
                                    }
                                } label: {
                                    Label("Compute tweak", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !spResult.isEmpty {
                                    Text(spResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("ASMap", systemImage: "map.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("ASMap hex", text: $asmapHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("IP", text: $asmapIp)
                                    .textFieldStyle(.roundedBorder)
                                    .frame(width: 120)
                                Button {
                                    do {
                                        let ip16 = try ip16ForLookup(ipStr: asmapIp)
                                        let sane = try asmapSanityCheck(asmapHex: asmapHex)
                                        let asn = try asmapInterpret(asmapHex: asmapHex, ip16: ip16)
                                        asmapResult = "sane=\(sane) asn=\(asn)"
                                    } catch {
                                        asmapResult = "error"
                                    }
                                } label: {
                                    Label("Lookup ASN", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !asmapResult.isEmpty {
                                    Text(asmapResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Query internals", systemImage: "gearshape.2.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-query")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                do {
                                    let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-query-int")
                                    let drain = query.drainAndFenceHi()
                                    let sh = query.shIndexedThroughHeight()
                                    let maxSh = query.maxShCreates()
                                    let recon = query.sampleResetReconstructArchived()
                                    let thin = query.sampleResetThinTweakBodyBytes()
                                    queryDrainResult = "drain=\(drain?.description ?? "none")"
                                    queryShResult = "sh=\(sh?.description ?? "none") maxSh=\(maxSh) recon=\(recon) thin=\(thin)"
                                } catch {
                                    queryDrainResult = "error"
                                    queryShResult = ""
                                }
                            } label: {
                                Label("Load internals", systemImage: "arrow.up.doc.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !queryDrainResult.isEmpty {
                                Text(queryDrainResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                            if !queryShResult.isEmpty {
                                Text(queryShResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText.opacity(0.68))
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Block structure", systemImage: "cube.transparent")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Block hex", text: $blockStructHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = blockStructHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty else {
                                        blockStructResult = ""
                                        return
                                    }
                                    do {
                                        try validateBlockStructure(blockHex: hex, network: "regtest", height: 0, enforceHeightGates: true)
                                        blockStructResult = "Valid structure"
                                    } catch {
                                        blockStructResult = "Invalid structure"
                                    }
                                } label: {
                                    Label("Validate", systemImage: "checkmark.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !blockStructResult.isEmpty {
                                    Text(blockStructResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Netgroup", systemImage: "network")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            HStack(spacing: 8) {
                                TextField("IP", text: $netgroupIp)
                                    .textFieldStyle(.roundedBorder)
                                    .frame(width: 120)
                                TextField("Port", text: $netgroupPort)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 80)
                                Button {
                                    guard let port = UInt16(netgroupPort) else {
                                        netgroupResult = "invalid port"
                                        return
                                    }
                                    do {
                                        let group = try netgroup(ip: netgroupIp, port: port, asmapHex: nil)
                                        netgroupResult = "group: \(group)"
                                    } catch {
                                        netgroupResult = "error"
                                    }
                                } label: {
                                    Label("Lookup", systemImage: "arrow.right.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !netgroupResult.isEmpty {
                                Text(netgroupResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Work sum", systemImage: "sum")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Work hexes (comma-separated)", text: $workHexes)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hexes = workHexes.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces) }
                                    guard !hexes.isEmpty else {
                                        workResult = ""
                                        return
                                    }
                                    do {
                                        let sum = try sumWorkHex(workHexes: hexes)
                                        workResult = sum
                                    } catch {
                                        workResult = "invalid"
                                    }
                                } label: {
                                    Label("Sum", systemImage: "plus.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !workResult.isEmpty {
                                    Text(workResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("SP tweaks at height", systemImage: "ear.badge.waveform")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            HStack(spacing: 8) {
                                TextField("Height", text: $tweaksHeight)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 80)
                                Button {
                                    guard let height = UInt32(tweaksHeight) else {
                                        tweaksResult = "invalid height"
                                        return
                                    }
                                    do {
                                        let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-query-sp")
                                        let tweaks = try query.tweaksAtHeight(network: "mainnet", height: height)
                                        tweaksResult = "\(tweaks.count) tweaks"
                                    } catch {
                                        tweaksResult = "error"
                                    }
                                } label: {
                                    Label("Load", systemImage: "arrow.down.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !tweaksResult.isEmpty {
                                Text(tweaksResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 10) {
                            Label("What this proves", systemImage: "checkmark.seal.fill")
                                .font(.headline)
                                .foregroundStyle(accentText)

                            ForEach([
                                "SwiftUI rendering",
                                "State-driven interactions",
                                "Native Rust function calls",
                                "Real rbitcoin primitives",
                                "Consensus verification",
                                "Store + Query FFI",
                                "Mempool + Fee estimation",
                                "Tx parsing + Block hash",
                                "P2P network + Electrum",
                                "Log control + Genesis",
                                "Seed resolution + Constants",
                                "Regtest mining + Service flags",
                                "Versionbits + Merkle root",
                                "Query stats + Archive probe",
                                "Header hash + Fee at rate",
                                "Witness commitment + Sigops",
                                "BIP68 check + Libre policy",
                                "Regtest pay mining + Seed services",
                                "Query tx lookup + Warnings",
                                "Silent payments + ASMap",
                                "Query internals",
                                "Block structure + Netgroup",
                                "Work sum + SP tweaks",
                            ], id: \.self) { item in
                                HStack(spacing: 10) {
                                    Image(systemName: "checkmark.circle.fill")
                                        .foregroundStyle(accentFill)
                                    Text(item)
                                        .foregroundStyle(primaryText.opacity(colorScheme == .dark ? 0.88 : 0.84))
                                    Spacer()
                                }
                                .font(.subheadline)
                            }
                        }
                    }
                }
                .padding(20)
            }
        }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .top) {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Swifty Rust")
                        .font(.system(size: 34, weight: .bold, design: .rounded))
                        .foregroundStyle(primaryText)

                    Text("A polished SwiftUI shell with the same warm tone as the new icon.")
                        .font(.subheadline)
                        .foregroundStyle(primaryText.opacity(0.74))
                }

                Spacer()

                Image(colorScheme == .dark ? "RustOrb" : "RustOrbLight")
                    .resizable()
                    .scaledToFit()
                    .frame(width: 76, height: 76)
                    .padding(12)
                    .background(
                        colorScheme == .dark
                            ? .white.opacity(0.04)
                            : .white.opacity(0.78),
                        in: RoundedRectangle(cornerRadius: 22, style: .continuous)
                    )
                    .overlay(
                        RoundedRectangle(cornerRadius: 22, style: .continuous)
                            .stroke(
                                colorScheme == .dark
                                    ? Color(red: 1.0, green: 0.66, blue: 0.24).opacity(0.26)
                                    : Color(red: 0.52, green: 0.62, blue: 0.72).opacity(0.22),
                                lineWidth: 1
                            )
                    )
            }

            HStack(spacing: 8) {
                pill(text: "SwiftUI")
                pill(text: "UniFFI")
                pill(text: "Rust")
            }
        }
        .padding(.bottom, 4)
    }

    private func pill(text: String) -> some View {
        Text(text)
            .font(.caption.weight(.semibold))
            .padding(.horizontal, 12)
            .padding(.vertical, 7)
            .background(accentFill.opacity(colorScheme == .dark ? 0.14 : 0.10), in: Capsule())
            .foregroundStyle(accentText)
    }

    private func glassCard<Content: View>(@ViewBuilder content: () -> Content) -> some View {
        content()
            .padding(18)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(cardBackground, in: RoundedRectangle(cornerRadius: 24, style: .continuous))
            .overlay(
                RoundedRectangle(cornerRadius: 24, style: .continuous)
                    .stroke(accentFill.opacity(colorScheme == .dark ? 0.20 : 0.14), lineWidth: 1)
            )
            .shadow(color: .black.opacity(colorScheme == .dark ? 0.28 : 0.12), radius: 18, x: 0, y: 10)
    }

    private func updateAddressResult() {
        if validateAddress(address: addressInput) {
            do {
                addressNetworkResult = try addressNetwork(address: addressInput)
            } catch {
                addressNetworkResult = "error"
            }
        } else {
            addressNetworkResult = ""
        }
    }

    private func updateHashResult() {
        let data = Data(hashInput.utf8)
        hashResult = hash256(bytes: data)
    }

    private func updateSubsidy() {
        guard let height = UInt32(subsidyHeight) else {
            subsidyResult = ""
            return
        }
        do {
            let satoshis = try blockSubsidy(height: height, network: subsidyNetwork)
            let btc = Double(satoshis) / 100_000_000.0
            subsidyResult = String(format: "%.8f BTC", btc)
        } catch {
            subsidyResult = "error"
        }
    }

    private func updateBlockValidation() {
        let hex = blockHexInput.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !hex.isEmpty else {
            blockValidationResult = ""
            return
        }
        do {
            try checkBlockWire(blockHex: hex)
            blockValidationResult = "Valid"
        } catch {
            blockValidationResult = "Invalid"
        }
    }

    private func updateFeeResult() {
        guard let nBlocks = UInt32(feeTargetBlocks), let stock = UInt64(feeStockAbove) else {
            feeResult = ""
            return
        }
        let inflow = Array(repeating: UInt64(0), count: Int(feeBucketCount()))
        let candidates = feeDefaultCandidateRates()
        if let rate = feeMinRateForCapacitySimple(stockAbove: stock, inflowWuPerSByBucket: inflow, nBlocks: nBlocks, candidateRates: candidates) {
            feeResult = "\(rate) sat/kvB"
        } else {
            feeResult = "no fit"
        }
    }

    private func updateTxParse() {
        let hex = txHexInput.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !hex.isEmpty else {
            txParseResult = ""
            return
        }
        do {
            let info = try parseTx(txHex: hex)
            txParseResult = "txid: \(info.txid.prefix(16))… v\(info.version) \(info.inputCount)in \(info.outputCount)out"
        } catch {
            txParseResult = "invalid tx"
        }
    }

    private func updateHeaderHash() {
        let hex = headerHexInput.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !hex.isEmpty else {
            headerHashResult = ""
            return
        }
        do {
            let hash = try blockHashFromHeader(headerHex: hex)
            headerHashResult = hash
        } catch {
            headerHashResult = "invalid header"
        }
    }

    private func updateScriptHash() {
        let hex = scriptHashInput.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !hex.isEmpty else {
            scriptHashResult = ""
            return
        }
        do {
            let hash = try electrumScripthashHex(scriptHex: hex)
            scriptHashResult = hash
        } catch {
            scriptHashResult = "invalid script"
        }
    }

    private func stepperRow(title: String, value: Binding<Int>, range: ClosedRange<Int>) -> some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text(title)
                    .font(.subheadline.weight(.semibold))
                    .foregroundStyle(primaryText.opacity(0.92))
                Text("Tap +/- or use the randomizer")
                    .font(.caption)
                    .foregroundStyle(primaryText.opacity(0.62))
            }

            Spacer()

            Stepper(value: value, in: range) {
                Text("\(value.wrappedValue)")
                    .font(.title3.weight(.semibold))
                    .monospacedDigit()
                    .foregroundStyle(primaryText)
                    .frame(minWidth: 44, alignment: .trailing)
            }
            .labelsHidden()
            .tint(accentFill)
        }
    }

    private var primaryText: Color {
        colorScheme == .dark ? .white : Color(red: 0.10, green: 0.14, blue: 0.20)
    }

    private var accentFill: Color {
        colorScheme == .dark ? Color(red: 1.0, green: 0.66, blue: 0.24) : Color(red: 0.82, green: 0.38, blue: 0.10)
    }

    private var accentText: Color {
        colorScheme == .dark ? Color(red: 1.0, green: 0.86, blue: 0.62) : Color(red: 0.42, green: 0.24, blue: 0.12)
    }

    private var cardBackground: Color {
        colorScheme == .dark ? .white.opacity(0.05) : .white.opacity(0.76)
    }
}

private struct PrimaryButtonStyle: ButtonStyle {
    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .font(.headline.weight(.semibold))
            .padding(.vertical, 14)
            .foregroundStyle(.white)
            .background(
                LinearGradient(
                    colors: [
                        Color(red: 1.0, green: 0.56, blue: 0.12),
                        Color(red: 0.93, green: 0.30, blue: 0.08)
                    ],
                    startPoint: .leading,
                    endPoint: .trailing
                ),
                in: RoundedRectangle(cornerRadius: 16, style: .continuous)
            )
            .scaleEffect(configuration.isPressed ? 0.98 : 1.0)
            .opacity(configuration.isPressed ? 0.92 : 1.0)
    }
}

#Preview {
    ContentView()
}
