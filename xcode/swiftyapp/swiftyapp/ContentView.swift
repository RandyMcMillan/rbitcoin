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
    @State private var rbfNewFee = "2000"
    @State private var rbfNewWeight = "1000"
    @State private var rbfOldFee = "1000"
    @State private var rbfOldWeight = "1000"
    @State private var rbfResult = ""
    @State private var queryHashInput = ""
    @State private var queryHeightResult = ""
    @State private var v2ContentsHex = ""
    @State private var v2Result = ""
    @State private var queryModeResult = ""
    @State private var queryBackfillResult = ""
    @State private var peerAddrInput = "127.0.0.1:8333"
    @State private var peerAddrResult = ""
    @State private var connectBlockHex = ""
    @State private var connectBlockResult = ""
    @State private var mtpHeight = "0"
    @State private var mtpResult = ""
    @State private var disconnectHeight = "100"
    @State private var disconnectHash = "0000000000000000000000000000000000000000000000000000000000000000"
    @State private var disconnectTxCount = "5"
    @State private var disconnectResult = ""
    @State private var servePerfResult = ""
    @State private var queryLoadResult = ""
    @State private var scriptForksTxHex = ""
    @State private var scriptForksResult = ""
    @State private var classAHeight = "1"
    @State private var classABlockHex = ""
    @State private var classAResult = ""
    @State private var headerRecordHex = ""
    @State private var headerRecordHash = ""
    @State private var headerRecordResult = ""
    @State private var evictionResult = ""
    @State private var pinViewResult = ""
    @State private var tipTimeInput = "1000"
    @State private var tipNowInput = "1000"
    @State private var tipFutureResult = ""
    @State private var unspendableScript = "6a"
    @State private var unspendableResult = ""
    @State private var txoutHexInput = "00f2052a010000001976a914000000000000000000000000000000000000000088ac"
    @State private var txoutSizeResult = ""
    @State private var medianScores = "1, 3, 2"
    @State private var medianResult = ""
    @State private var percentileScores = "1, 2, 3, 4, 5"
    @State private var percentileWeights = "10, 10, 10, 10, 10"
    @State private var percentileResult = ""
    @State private var workNewHex = "0000000000000000000000000000000000000000000000000000000000000002"
    @State private var workOldHex = "0000000000000000000000000000000000000000000000000000000000000001"
    @State private var workBetterResult = ""
    @State private var badPrevErr = "unexpected previous header"
    @State private var badPrevResult = ""
    @State private var timeoutNow = "1000"
    @State private var timeoutBest = "500"
    @State private var timeoutResult = ""
    @State private var staleIds = "1, 2, 3"
    @State private var staleGroups = "10, 20, 10"
    @State private var staleSalt = "0"
    @State private var staleResult = ""
    @State private var witnessBlockHex = ""
    @State private var witnessCommitResult = ""
    @State private var dnsSeedInput = "seed.bitcoin.sipa.be"
    @State private var dnsSeedServices = "0"
    @State private var dnsSeedResult = ""
    @State private var candidateBlockHex = ""
    @State private var candidatePrevHash = "0000000000000000000000000000000000000000000000000000000000000000"
    @State private var candidateTime = "1296688602"
    @State private var candidateResult = ""
    @State private var lastHeightStart = "0"
    @State private var lastHeightCount = "10"
    @State private var lastHeightTip = "5"
    @State private var lastHeightResult = ""
    @State private var sealElapsed = "10"
    @State private var sealBudget = "5"
    @State private var sealResult = ""
    @State private var shHashInput = "76a914000000000000000000000000000000000000000088ac"
    @State private var shHashResult = ""
    @State private var softDensifyResult = ""
    @State private var esploraScriptInput = "76a914000000000000000000000000000000000000000088ac"
    @State private var esploraScriptNetwork = "mainnet"
    @State private var esploraScriptResult = ""
    @State private var esploraPerfResult = ""
    @State private var peerConstantsResult = ""
    @State private var peerLogResult = ""
    @State private var moreConstantsResult = ""
    @State private var keypairNetwork = "mainnet"
    @State private var keypairResult = ""
    @State private var keypairAddress = ""
    @State private var bip32Seed = "000102030405060708090a0b0c0d0e0f"
    @State private var bip32Network = "mainnet"
    @State private var bip32Xpriv = ""
    @State private var bip32Xpub = ""
    @State private var bip32Address = ""
    @State private var psbtHex = ""
    @State private var psbtTxResult = ""
    @State private var psbtFeeResult = ""
    @State private var chainConstantsResult = ""
    @State private var chainLogResult = ""
    @State private var v2ConstantsResult = ""
    @State private var moreConstants2Result = ""

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
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("RBF check", systemImage: "arrow.triangle.2.circlepath")
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
                                HStack(spacing: 8) {
                                    TextField("New fee", text: $rbfNewFee)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                    TextField("New weight", text: $rbfNewWeight)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                }
                                HStack(spacing: 8) {
                                    TextField("Old fee", text: $rbfOldFee)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                    TextField("Old weight", text: $rbfOldWeight)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                }
                                Button {
                                    guard let newFee = UInt64(rbfNewFee), let newWeight = UInt64(rbfNewWeight),
                                          let oldFee = UInt64(rbfOldFee), let oldWeight = UInt64(rbfOldWeight) else {
                                        rbfResult = "invalid input"
                                        return
                                    }
                                    let pays = rbfPaysForReplacement(newFee: newFee, newWeight: newWeight, oldFee: oldFee, oldWeight: oldWeight)
                                    let rbfr = pureRbfrPays(newFee: newFee, newWeight: newWeight, directFee: oldFee, directWeight: oldWeight)
                                    let allows = rbfAllowsReplacement(newFee: newFee, newWeight: newWeight, conflictFee: oldFee, conflictWeight: oldWeight, directFee: oldFee, directWeight: oldWeight)
                                    rbfResult = "pays=\(pays) rbfr=\(rbfr) allows=\(allows)"
                                } label: {
                                    Label("Check RBF", systemImage: "checkmark.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !rbfResult.isEmpty {
                                    Text(rbfResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Query chain view", systemImage: "link")
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
                                TextField("Block hash hex", text: $queryHashInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    do {
                                        let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-query-cv")
                                        let height = try query.heightOfHash(hashHex: queryHashInput)
                                        let header = try query.headerAtHeight(height: 0)
                                        queryHeightResult = "height: \(height?.description ?? "none") header: \(header != nil ? "found" : "none")"
                                    } catch {
                                        queryHeightResult = "error"
                                    }
                                } label: {
                                    Label("Lookup", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !queryHeightResult.isEmpty {
                                    Text(queryHeightResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("V2 transport", systemImage: "network.badge.shield.half.filled")
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
                                TextField("Contents hex", text: $v2ContentsHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = v2ContentsHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty else {
                                        v2Result = ""
                                        return
                                    }
                                    do {
                                        try parseV2Regtest(contentsHex: hex)
                                        v2Result = "valid v2"
                                    } catch {
                                        v2Result = "invalid v2"
                                    }
                                } label: {
                                    Label("Parse", systemImage: "checkmark.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !v2Result.isEmpty {
                                    Text(v2Result)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Query mode", systemImage: "switch.2")
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
                                    let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-query-mode")
                                    let mode = query.indexMode()
                                    let sh = query.shIndexEnabled()
                                    queryModeResult = "mode=\(mode) sh=\(sh)"
                                } catch {
                                    queryModeResult = "error"
                                }
                            } label: {
                                Label("Load mode", systemImage: "arrow.up.doc.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !queryModeResult.isEmpty {
                                Text(queryModeResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("SP backfill", systemImage: "ear.badge.waveform")
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

                            Button {
                                do {
                                    let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-query-backfill")
                                    let count = try query.backfillSpTweaks(network: "mainnet")
                                    queryBackfillResult = "backfilled: \(count)"
                                } catch {
                                    queryBackfillResult = "error"
                                }
                            } label: {
                                Label("Backfill", systemImage: "arrow.triangle.2.circlepath")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !queryBackfillResult.isEmpty {
                                Text(queryBackfillResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Peer address", systemImage: "network")
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
                                TextField("Address", text: $peerAddrInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.body, design: .monospaced))
                                Button {
                                    do {
                                        let addr = try parsePeerAddr(addr: peerAddrInput)
                                        peerAddrResult = addr
                                    } catch {
                                        peerAddrResult = "invalid"
                                    }
                                } label: {
                                    Label("Parse", systemImage: "checkmark.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !peerAddrResult.isEmpty {
                                Text(peerAddrResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Accept & connect", systemImage: "link")
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
                                TextField("Block hex", text: $connectBlockHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = connectBlockHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty else {
                                        connectBlockResult = ""
                                        return
                                    }
                                    do {
                                        let fk = try acceptAndConnectBlock(queryPath: NSTemporaryDirectory() + "rbitcoin-connect-demo", network: "regtest", height: 1, blockHex: hex, milestoneHeight: 0)
                                        connectBlockResult = "fk=\(fk)"
                                    } catch {
                                        connectBlockResult = "rejected"
                                    }
                                } label: {
                                    Label("Connect", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !connectBlockResult.isEmpty {
                                    Text(connectBlockResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("MTP", systemImage: "clock")
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
                                TextField("Height", text: $mtpHeight)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 80)
                                Button {
                                    guard let height = UInt32(mtpHeight) else {
                                        mtpResult = "invalid"
                                        return
                                    }
                                    do {
                                        let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-mtp-demo")
                                        let mtp = try query.medianTimePast(height: height)
                                        mtpResult = "\(mtp)"
                                    } catch {
                                        mtpResult = "error"
                                    }
                                } label: {
                                    Label("MTP", systemImage: "arrow.down.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !mtpResult.isEmpty {
                                Text(mtpResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Disconnect tip", systemImage: "arrow.uturn.backward")
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

                            HStack(spacing: 8) {
                                TextField("Height", text: $disconnectHeight)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 80)
                                TextField("Tx count", text: $disconnectTxCount)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 80)
                            }
                            TextField("Hash", text: $disconnectHash)
                                .textFieldStyle(.roundedBorder)
                                .font(.system(.caption, design: .monospaced))
                            Button {
                                guard let height = UInt32(disconnectHeight), let nTx = UInt32(disconnectTxCount) else {
                                    disconnectResult = "invalid input"
                                    return
                                }
                                do {
                                    let line = try formatDisconnectTipLine(height: height, hashHex: disconnectHash, nTx: nTx)
                                    disconnectResult = line
                                } catch {
                                    disconnectResult = "invalid hash"
                                }
                            } label: {
                                Label("Format", systemImage: "text.quote")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())
                            if !disconnectResult.isEmpty {
                                Text(disconnectResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                                    .lineLimit(1)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Serve perf", systemImage: "chart.line.uptrend.xyaxis")
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
                                let sample = sampleResetServePerf()
                                servePerfResult = formatServePerf(sample: sample)
                            } label: {
                                Label("Sample perf", systemImage: "arrow.down.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())
                            if !servePerfResult.isEmpty {
                                Text(servePerfResult)
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundStyle(primaryText.opacity(0.74))
                                    .lineLimit(1)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Query load pack", systemImage: "arrow.up.doc.fill")
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
                                    let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-query-load")
                                    try query.onLoadPack()
                                    queryLoadResult = "ok"
                                } catch {
                                    queryLoadResult = "error"
                                }
                            } label: {
                                Label("Load pack", systemImage: "arrow.up.doc.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())
                            if !queryLoadResult.isEmpty {
                                Text(queryLoadResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Script verify forks", systemImage: "checkmark.shield.fill")
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
                                TextField("Tx hex", text: $scriptForksTxHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = scriptForksTxHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty else {
                                        scriptForksResult = ""
                                        return
                                    }
                                    do {
                                        try verifyTxScriptsDetachedForks(prevoutsHex: [], txHex: hex, bip65Active: true, bip112Active: true, bip66Active: true, bip16Active: true, taprootActive: true)
                                        scriptForksResult = "scripts valid"
                                    } catch {
                                        scriptForksResult = "scripts invalid"
                                    }
                                } label: {
                                    Label("Verify", systemImage: "checkmark.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !scriptForksResult.isEmpty {
                                    Text(scriptForksResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Class A commit", systemImage: "archivebox.fill")
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
                                TextField("Block hex", text: $classABlockHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                HStack(spacing: 8) {
                                    TextField("Height", text: $classAHeight)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                    Button {
                                        let hex = classABlockHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                        guard !hex.isEmpty, let height = UInt32(classAHeight) else {
                                            classAResult = "invalid input"
                                            return
                                        }
                                        do {
                                            try commitClassABlock(queryPath: NSTemporaryDirectory() + "rbitcoin-class-a-demo", network: "regtest", height: height, blockHex: hex, milestoneHeight: 0)
                                            classAResult = "committed"
                                        } catch {
                                            classAResult = "rejected"
                                        }
                                    } label: {
                                        Label("Commit", systemImage: "arrow.right.circle.fill")
                                    }
                                    .buttonStyle(PrimaryButtonStyle())
                                    Spacer()
                                }
                                if !classAResult.isEmpty {
                                    Text(classAResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Header to record", systemImage: "doc.text.fill")
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
                                TextField("Header hex", text: $headerRecordHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("Hash hex", text: $headerRecordHash)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = headerRecordHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    let hash = headerRecordHash.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty, !hash.isEmpty else {
                                        headerRecordResult = ""
                                        return
                                    }
                                    do {
                                        let rec = try headerToRecord(prevFk: 0, headerHex: hex, hashHex: hash)
                                        headerRecordResult = "ver=\(rec.version) time=\(rec.timestamp) bits=\(rec.bits)"
                                    } catch {
                                        headerRecordResult = "invalid"
                                    }
                                } label: {
                                    Label("Convert", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !headerRecordResult.isEmpty {
                                    Text(headerRecordResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Inbound eviction", systemImage: "person.2.badge.gearshape.fill")
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
                                var cands: [FfiInboundEvictCandidate] = []
                                for i in 0..<30 {
                                    cands.append(FfiInboundEvictCandidate(
                                        id: UInt64(i + 1),
                                        connectedAt: UInt64(i * 10),
                                        minPing: Double(i),
                                        lastBlock: UInt64(i),
                                        lastTx: UInt64(i),
                                        netgroup: UInt64(i),
                                        noban: false
                                    ))
                                }
                                let evicted = selectInboundEviction(candidates: cands)
                                evictionResult = evicted != nil ? "evicted: \(evicted!)" : "all protected"
                            } label: {
                                Label("Evict", systemImage: "arrow.right.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())
                            if !evictionResult.isEmpty {
                                Text(evictionResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Pin chain view", systemImage: "pin.fill")
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
                                    let query = try FfiQuery.openOrCreate(path: NSTemporaryDirectory() + "rbitcoin-pin-demo")
                                    let view = try query.pinChainView()
                                    let shView = try query.pinShChainView()
                                    pinViewResult = "view: \(view != nil ? "pinned" : "none") sh: \(shView != nil ? "pinned" : "none")"
                                } catch {
                                    pinViewResult = "error"
                                }
                            } label: {
                                Label("Pin views", systemImage: "pin.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())
                            if !pinViewResult.isEmpty {
                                Text(pinViewResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Tip future check", systemImage: "clock.arrow.circlepath")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-node")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            HStack(spacing: 8) {
                                TextField("Tip time", text: $tipTimeInput)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 100)
                                TextField("Now", text: $tipNowInput)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 100)
                                Button {
                                    guard let tip = UInt32(tipTimeInput), let now = UInt64(tipNowInput) else {
                                        tipFutureResult = "invalid"
                                        return
                                    }
                                    let far = tipTooFarInFuture(tipTime: tip, now: now)
                                    let max = maxFutureBlockTime()
                                    tipFutureResult = far ? "too far (>\(max)s)" : "ok"
                                } label: {
                                    Label("Check", systemImage: "checkmark.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !tipFutureResult.isEmpty {
                                Text(tipFutureResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Unspendable script", systemImage: "xmark.shield.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-rpc")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Script hex", text: $unspendableScript)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                    .onChange(of: unspendableScript) { _ in
                                        unspendableResult = isUnspendable(scriptHex: unspendableScript) ? "unspendable" : "spendable"
                                    }
                                if !unspendableResult.isEmpty {
                                    HStack(spacing: 6) {
                                        Image(systemName: unspendableResult == "unspendable" ? "xmark.circle.fill" : "checkmark.circle.fill")
                                            .foregroundStyle(unspendableResult == "unspendable" ? Color.red : Color.green)
                                        Text(unspendableResult)
                                            .font(.caption.weight(.semibold))
                                    }
                                }
                            }
                        }
                    }
                    .onAppear {
                        unspendableResult = isUnspendable(scriptHex: unspendableScript) ? "unspendable" : "spendable"
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("TxOut size", systemImage: "doc.text.magnifyingglass")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-rpc")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("TxOut hex", text: $txoutHexInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = txoutHexInput.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty else {
                                        txoutSizeResult = ""
                                        return
                                    }
                                    do {
                                        let size = try txoutSerializedSize(outHex: hex)
                                        txoutSizeResult = "\(size) bytes"
                                    } catch {
                                        txoutSizeResult = "invalid"
                                    }
                                } label: {
                                    Label("Size", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !txoutSizeResult.isEmpty {
                                    Text(txoutSizeResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Truncated median", systemImage: "function")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-rpc")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Scores (comma-separated)", text: $medianScores)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let parts = medianScores.split(separator: ",").compactMap { Int64($0.trimmingCharacters(in: .whitespaces)) }
                                    guard !parts.isEmpty else {
                                        medianResult = ""
                                        return
                                    }
                                    let med = truncatedMedian(scores: parts)
                                    medianResult = "\(med)"
                                } label: {
                                    Label("Median", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !medianResult.isEmpty {
                                    Text(medianResult)
                                        .font(.title3.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Percentiles by weight", systemImage: "chart.bar.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-rpc")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Scores (comma-separated)", text: $percentileScores)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("Weights (comma-separated)", text: $percentileWeights)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let s = percentileScores.split(separator: ",").compactMap { Int64($0.trimmingCharacters(in: .whitespaces)) }
                                    let w = percentileWeights.split(separator: ",").compactMap { Int64($0.trimmingCharacters(in: .whitespaces)) }
                                    guard s.count == w.count, !s.isEmpty else {
                                        percentileResult = "mismatch"
                                        return
                                    }
                                    do {
                                        let p = try percentilesByWeight(scores: s, weights: w, totalWeight: w.reduce(0, +))
                                        percentileResult = p.map { String($0) }.joined(separator: ", ")
                                    } catch {
                                        percentileResult = "error"
                                    }
                                } label: {
                                    Label("Compute", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !percentileResult.isEmpty {
                                    Text(percentileResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Work comparison", systemImage: "scale.3d")
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
                                TextField("New work hex", text: $workNewHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("Old work hex", text: $workOldHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let new = workNewHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    let old = workOldHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !new.isEmpty, !old.isEmpty else {
                                        workBetterResult = ""
                                        return
                                    }
                                    do {
                                        let better = try workBetter(newWorkHex: new, oldWorkHex: old)
                                        workBetterResult = better ? "new > old" : "new ≤ old"
                                    } catch {
                                        workBetterResult = "invalid"
                                    }
                                } label: {
                                    Label("Compare", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !workBetterResult.isEmpty {
                                    Text(workBetterResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Bad prev error", systemImage: "exclamationmark.triangle.fill")
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
                                TextField("Error text", text: $badPrevErr)
                                    .textFieldStyle(.roundedBorder)
                                    .onChange(of: badPrevErr) { _ in
                                        badPrevResult = isBadPrevErr(err: badPrevErr) ? "bad prev" : "other"
                                    }
                                if !badPrevResult.isEmpty {
                                    HStack(spacing: 6) {
                                        Image(systemName: badPrevResult == "bad prev" ? "xmark.circle.fill" : "checkmark.circle.fill")
                                            .foregroundStyle(badPrevResult == "bad prev" ? Color.red : Color.green)
                                        Text(badPrevResult)
                                            .font(.caption.weight(.semibold))
                                    }
                                }
                            }
                        }
                    }
                    .onAppear {
                        badPrevResult = isBadPrevErr(err: badPrevErr) ? "bad prev" : "other"
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Header timeout", systemImage: "clock.badge.exclamationmark.fill")
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
                                TextField("Now", text: $timeoutNow)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 100)
                                TextField("Best header", text: $timeoutBest)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 100)
                                Button {
                                    guard let now = UInt64(timeoutNow), let best = UInt64(timeoutBest) else {
                                        timeoutResult = "invalid"
                                        return
                                    }
                                    let t = headersDownloadTimeoutSecs(now: now, bestHeaderTime: best)
                                    timeoutResult = "\(t)"
                                } label: {
                                    Label("Timeout", systemImage: "arrow.right.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !timeoutResult.isEmpty {
                                Text(timeoutResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Stale follow evict", systemImage: "person.2.badge.minus")
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
                                TextField("IDs (comma-separated)", text: $staleIds)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("Groups (comma-separated)", text: $staleGroups)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                HStack(spacing: 8) {
                                    TextField("Salt", text: $staleSalt)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 80)
                                    Button {
                                        let idParts = staleIds.split(separator: ",").compactMap { UInt64($0.trimmingCharacters(in: .whitespaces)) }
                                        let groupParts = staleGroups.split(separator: ",").compactMap { UInt64($0.trimmingCharacters(in: .whitespaces)) }
                                        guard let salt = UInt64(staleSalt), !idParts.isEmpty else {
                                            staleResult = "invalid"
                                            return
                                        }
                                        let evicted = pickStaleFollowEvict(ids: idParts, salt: salt, groups: groupParts)
                                        staleResult = evicted != nil ? "evicted: \(evicted!)" : "none"
                                    } label: {
                                        Label("Evict", systemImage: "arrow.right.circle.fill")
                                    }
                                    .buttonStyle(PrimaryButtonStyle())
                                    Spacer()
                                }
                                if !staleResult.isEmpty {
                                    Text(staleResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Witness commitment", systemImage: "doc.badge.checkmark.fill")
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
                                TextField("Block hex", text: $witnessBlockHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    let hex = witnessBlockHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                    guard !hex.isEmpty else {
                                        witnessCommitResult = ""
                                        return
                                    }
                                    do {
                                        let result = try applyWitnessCommitment(blockHex: hex)
                                        witnessCommitResult = "committed: \(result.prefix(32))…"
                                    } catch {
                                        witnessCommitResult = "invalid"
                                    }
                                } label: {
                                    Label("Apply", systemImage: "arrow.right.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !witnessCommitResult.isEmpty {
                                    Text(witnessCommitResult)
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
                                Label("DNS seed query", systemImage: "globe")
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
                                TextField("Seed", text: $dnsSeedInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("Services", text: $dnsSeedServices)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 80)
                                Button {
                                    guard let services = UInt64(dnsSeedServices) else {
                                        dnsSeedResult = "invalid"
                                        return
                                    }
                                    let host = dnsSeedQueryHost(seed: dnsSeedInput, servicesU64: services)
                                    dnsSeedResult = host
                                } label: {
                                    Label("Query", systemImage: "arrow.right.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !dnsSeedResult.isEmpty {
                                Text(dnsSeedResult)
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundStyle(primaryText.opacity(0.74))
                                    .lineLimit(1)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Regtest candidate", systemImage: "hammer.fill")
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
                                TextField("Block hex", text: $candidateBlockHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                TextField("Prev hash", text: $candidatePrevHash)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                HStack(spacing: 8) {
                                    TextField("Time", text: $candidateTime)
                                        .textFieldStyle(.roundedBorder)
                                        .keyboardType(.numberPad)
                                        .frame(width: 100)
                                    Button {
                                        let hex = candidateBlockHex.trimmingCharacters(in: .whitespacesAndNewlines)
                                        guard !hex.isEmpty, let time = UInt32(candidateTime) else {
                                            candidateResult = "invalid input"
                                            return
                                        }
                                        do {
                                            let result = try prepareRegtestCandidate(blockHex: hex, prevHashHex: candidatePrevHash, time: time)
                                            candidateResult = "prepared: \(result.prefix(32))…"
                                        } catch {
                                            candidateResult = "invalid"
                                        }
                                    } label: {
                                        Label("Prepare", systemImage: "arrow.right.circle.fill")
                                    }
                                    .buttonStyle(PrimaryButtonStyle())
                                    Spacer()
                                }
                                if !candidateResult.isEmpty {
                                    Text(candidateResult)
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
                                Label("Last height", systemImage: "number")
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

                            HStack(spacing: 8) {
                                TextField("Start", text: $lastHeightStart)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 60)
                                TextField("Count", text: $lastHeightCount)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 60)
                                TextField("Tip", text: $lastHeightTip)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 60)
                                Button {
                                    guard let start = UInt32(lastHeightStart), let count = UInt32(lastHeightCount), let tip = UInt32(lastHeightTip) else {
                                        lastHeightResult = "invalid"
                                        return
                                    }
                                    let result = lastHeight(start: start, count: count, tip: tip)
                                    lastHeightResult = result != nil ? "\(result!)" : "none"
                                } label: {
                                    Label("Compute", systemImage: "arrow.right.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !lastHeightResult.isEmpty {
                                Text(lastHeightResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Seal subscribe", systemImage: "lock.shield.fill")
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

                            HStack(spacing: 8) {
                                TextField("Elapsed", text: $sealElapsed)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 80)
                                TextField("Budget", text: $sealBudget)
                                    .textFieldStyle(.roundedBorder)
                                    .keyboardType(.numberPad)
                                    .frame(width: 80)
                                Button {
                                    guard let elapsed = UInt64(sealElapsed), let budget = UInt64(sealBudget) else {
                                        sealResult = "invalid"
                                        return
                                    }
                                    let result = sealSubscribeChunk(elapsedSecs: elapsed, budgetSecs: budget, more: true)
                                    sealResult = result ? "seal" : "continue"
                                } label: {
                                    Label("Check", systemImage: "checkmark.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }
                            if !sealResult.isEmpty {
                                Text(sealResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Script hash", systemImage: "number")
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
                                TextField("Script hex", text: $shHashInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                    .onChange(of: shHashInput) { _ in
                                        shHashResult = scriptHashHex(scriptHex: shHashInput)
                                    }
                                if !shHashResult.isEmpty {
                                    Text(shHashResult)
                                        .font(.system(.caption, design: .monospaced))
                                        .foregroundStyle(primaryText.opacity(0.74))
                                        .lineLimit(1)
                                }
                            }
                        }
                    }
                    .onAppear {
                        shHashResult = scriptHashHex(scriptHex: shHashInput)
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Soft densify", systemImage: "slider.horizontal.3")
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
                                let window = softConfirmWindowN(rateBlocksPerS: 1.0)
                                let restricted = softAssignRestricted(depthBytes: 0)
                                let stopped = softAssignStopped(depthBytes: 0, stopBytes: UInt64.max)
                                let stopBytes = bqAssignStopBytes()
                                softDensifyResult = "window=\(window) restricted=\(restricted) stopped=\(stopped) stopBytes=\(stopBytes)"
                            } label: {
                                Label("Query soft densify", systemImage: "arrow.up.doc.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !softDensifyResult.isEmpty {
                                Text(softDensifyResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Esplora script", systemImage: "doc.text.magnifyingglass")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-esplora")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            VStack(alignment: .leading, spacing: 8) {
                                TextField("Script hex", text: $esploraScriptInput)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                HStack(spacing: 8) {
                                    Picker("Network", selection: $esploraScriptNetwork) {
                                        Text("mainnet").tag("mainnet")
                                        Text("testnet").tag("testnet")
                                        Text("regtest").tag("regtest")
                                        Text("signet").tag("signet")
                                    }
                                    .pickerStyle(.menu)
                                    .tint(accentText)
                                    Button {
                                        do {
                                            let fields = try esploraScriptFields(scriptHex: esploraScriptInput, network: esploraScriptNetwork)
                                            esploraScriptResult = "type=\(fields.scriptType) addr=\(fields.address ?? "none")"
                                        } catch {
                                            esploraScriptResult = "error"
                                        }
                                    } label: {
                                        Label("Project", systemImage: "arrow.right.circle.fill")
                                    }
                                    .buttonStyle(PrimaryButtonStyle())
                                    Spacer()
                                }
                                if !esploraScriptResult.isEmpty {
                                    Text(esploraScriptResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Esplora perf", systemImage: "chart.line.uptrend.xyaxis")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-esplora")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                let sample = sampleResetEsploraPerf()
                                esploraPerfResult = "req=\(sample.requests) bytes=\(sample.bytes) ms=\(sample.elapsedMs)"
                            } label: {
                                Label("Sample reset perf", systemImage: "arrow.counterclockwise.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !esploraPerfResult.isEmpty {
                                Text(esploraPerfResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Peer constants", systemImage: "antenna.radiowaves.left.and.right")
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
                                let ban = banScoreThreshold()
                                let maxServe = maxServeBlocks()
                                let minVer = minPeerProtoVersion()
                                let timeout = handshakeTimeoutSecs()
                                peerConstantsResult = "ban=\(ban) maxServe=\(maxServe) minVer=\(minVer) timeout=\(timeout)s"
                            } label: {
                                Label("Load peer constants", systemImage: "info.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !peerConstantsResult.isEmpty {
                                Text(peerConstantsResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }

                            Button {
                                let log1 = feelerConnectionCompletedLog()
                                let log2 = versionHandshakeTimeoutLog(peer: 1)
                                let log3 = obsoleteVersionLog(version: 70015, peer: 1)
                                peerLogResult = "\(log1.prefix(20))… | \(log2.prefix(20))…"
                            } label: {
                                Label("Sample peer logs", systemImage: "doc.text.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !peerLogResult.isEmpty {
                                Text(peerLogResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("More constants", systemImage: "number.circle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-mempool + net")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                let bw = mempoolBlockWeightWu()
                                let spb = mempoolSecondsPerBlock()
                                let mpc = mempoolMaxPackageCount()
                                let mpw = mempoolMaxPackageWeight()
                                let rnum = mempoolRbfrRatioNum()
                                let rden = mempoolRbfrRatioDen()
                                let maxMsgs = peerDefaultMaxMsgsPerSec()
                                let maxBytes = peerDefaultMaxBytesPerSec()
                                let rlBan = peerRateLimitBanScore()
                                let osBan = peerOversizeBanScore()
                                let maxAddr = peerMaxAddrToSend()
                                let maxPct = peerMaxPctAddrToSend()
                                moreConstantsResult = "bw=\(bw) spb=\(spb) pkg=\(mpc)/\(mpw) rbfr=\(rnum)/\(rden) msgs=\(maxMsgs) bytes=\(maxBytes) rl=\(rlBan) os=\(osBan) addr=\(maxAddr)/\(maxPct)"
                            } label: {
                                Label("Load more constants", systemImage: "info.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !moreConstantsResult.isEmpty {
                                Text(moreConstantsResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Key & Address", systemImage: "key.fill")
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

                            HStack(spacing: 8) {
                                Picker("Network", selection: $keypairNetwork) {
                                    Text("mainnet").tag("mainnet")
                                    Text("testnet").tag("testnet")
                                    Text("regtest").tag("regtest")
                                    Text("signet").tag("signet")
                                }
                                .pickerStyle(.menu)
                                .tint(accentText)
                                Button {
                                    do {
                                        let kp = try generateKeypair(network: keypairNetwork)
                                        keypairResult = "pk: \(kp.publicKeyHex.prefix(16))…"
                                        let addr = try p2wpkhAddressFromPubkey(pubkeyHex: kp.publicKeyHex, network: keypairNetwork)
                                        keypairAddress = addr
                                    } catch {
                                        keypairResult = "error"
                                        keypairAddress = ""
                                    }
                                } label: {
                                    Label("Generate", systemImage: "arrow.clockwise.circle.fill")
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                Spacer()
                            }

                            if !keypairResult.isEmpty {
                                Text(keypairResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                            if !keypairAddress.isEmpty {
                                Text(keypairAddress)
                                    .font(.system(.caption, design: .monospaced))
                                    .foregroundStyle(primaryText.opacity(0.74))
                                    .lineLimit(1)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("BIP32 HD Wallet", systemImage: "arrow.branch")
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
                                TextField("Seed hex", text: $bip32Seed)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                HStack(spacing: 8) {
                                    Picker("Network", selection: $bip32Network) {
                                        Text("mainnet").tag("mainnet")
                                        Text("testnet").tag("testnet")
                                        Text("regtest").tag("regtest")
                                        Text("signet").tag("signet")
                                    }
                                    .pickerStyle(.menu)
                                    .tint(accentText)
                                    Button {
                                        do {
                                            let xpriv = try xprivFromSeed(seedHex: bip32Seed, network: bip32Network)
                                            let xpub = try xpubFromXpriv(xprivString: xpriv)
                                            let accountXpriv = try deriveXpriv(xprivString: xpriv, path: "m/44'/0'/0'")
                                            let accountXpub = try xpubFromXpriv(xprivString: accountXpriv)
                                            let addr = try p2wpkhAddressFromXpub(xpubString: accountXpub, path: "m/0/0", network: bip32Network)
                                            bip32Xpriv = xpriv.prefix(16) + "…"
                                            bip32Xpub = xpub.prefix(16) + "…"
                                            bip32Address = addr
                                        } catch {
                                            bip32Xpriv = "error"
                                            bip32Xpub = ""
                                            bip32Address = ""
                                        }
                                    } label: {
                                        Label("Derive", systemImage: "arrow.right.circle.fill")
                                    }
                                    .buttonStyle(PrimaryButtonStyle())
                                    Spacer()
                                }
                                if !bip32Xpriv.isEmpty {
                                    Text("xpriv: \(bip32Xpriv)")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                                if !bip32Xpub.isEmpty {
                                    Text("xpub: \(bip32Xpub)")
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                                if !bip32Address.isEmpty {
                                    Text(bip32Address)
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
                                Label("PSBT", systemImage: "doc.plaintext.fill")
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
                                TextField("PSBT hex", text: $psbtHex)
                                    .textFieldStyle(.roundedBorder)
                                    .font(.system(.caption, design: .monospaced))
                                Button {
                                    do {
                                        let _ = try psbtFromHex(hex: psbtHex)
                                        let txHex = try psbtExtractTxHex(hex: psbtHex)
                                        let fee = try psbtFeeSat(hex: psbtHex)
                                        psbtTxResult = "tx: \(txHex.prefix(16))…"
                                        psbtFeeResult = "fee: \(fee) sat"
                                    } catch {
                                        psbtTxResult = "invalid psbt"
                                        psbtFeeResult = ""
                                    }
                                } label: {
                                    Label("Parse PSBT", systemImage: "magnifyingglass.circle.fill")
                                        .frame(maxWidth: .infinity)
                                }
                                .buttonStyle(PrimaryButtonStyle())
                                if !psbtTxResult.isEmpty {
                                    Text(psbtTxResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                                if !psbtFeeResult.isEmpty {
                                    Text(psbtFeeResult)
                                        .font(.caption.weight(.semibold))
                                        .foregroundStyle(primaryText)
                                }
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("Chain constants", systemImage: "link.circle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-net + consensus")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                let tipAge = defaultMaxTipAgeSecs()
                                let feefilter = ibdFeefilterSatKvb()
                                let stale = staleRelayAgeLimitSecs()
                                let addrMan = maxAddrMan()
                                let minRelay = minRelayFeeRateSatPerKvb()
                                chainConstantsResult = "tipAge=\(tipAge)s feefilter=\(feefilter) stale=\(stale)s addrMan=\(addrMan) minRelay=\(minRelay)"
                            } label: {
                                Label("Load chain constants", systemImage: "info.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !chainConstantsResult.isEmpty {
                                Text(chainConstantsResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }

                            Button {
                                let log1 = synchronizingBlockheadersLog(height: 100)
                                let log2 = receivedTxLog()
                                chainLogResult = "\(log1.prefix(20))… | \(log2.prefix(20))…"
                            } label: {
                                Label("Sample chain logs", systemImage: "doc.text.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !chainLogResult.isEmpty {
                                Text(chainLogResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("V2 transport", systemImage: "network.badge.shield.half.filled")
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
                                let maxLen = maxV2ContentsLen()
                                let expansion = v2CipherExpansion()
                                let recv = v2OtherRecvBytes(contentsLen: 100)
                                v2ConstantsResult = "maxLen=\(maxLen) expansion=\(expansion) recv=\(recv)"
                            } label: {
                                Label("Load V2 constants", systemImage: "info.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !v2ConstantsResult.isEmpty {
                                Text(v2ConstantsResult)
                                    .font(.caption.weight(.semibold))
                                    .foregroundStyle(primaryText)
                            }
                        }
                    }

                    glassCard {
                        VStack(alignment: .leading, spacing: 16) {
                            HStack {
                                Label("More constants 2", systemImage: "number.circle.fill")
                                    .font(.headline)
                                    .foregroundStyle(accentText)
                                Spacer()
                                Text("rbitcoin-query + mempool")
                                    .font(.caption.weight(.semibold))
                                    .padding(.horizontal, 10)
                                    .padding(.vertical, 6)
                                    .background(accentFill.opacity(colorScheme == .dark ? 0.18 : 0.12), in: Capsule())
                                    .foregroundStyle(accentText)
                            }

                            Button {
                                let softFree = bqSoftFreeBytes()
                                let softConfirm = bqSoftConfirmSecs()
                                let admitHalf = mempoolAdmitHalfLifeSecs()
                                let warmAfter = mempoolWarmAfterSecs()
                                let warmAdmits = mempoolWarmAfterAdmits()
                                moreConstants2Result = "softFree=\(softFree) softConfirm=\(softConfirm) admitHalf=\(admitHalf) warm=\(warmAfter)/\(warmAdmits)"
                            } label: {
                                Label("Load more constants", systemImage: "info.circle.fill")
                                    .frame(maxWidth: .infinity)
                            }
                            .buttonStyle(PrimaryButtonStyle())

                            if !moreConstants2Result.isEmpty {
                                Text(moreConstants2Result)
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
                                "RBF check + Query chain view",
                                "V2 transport + Query mode",
                                "SP backfill",
                                "Peer address + Accept & connect",
                                "MTP",
                                "Disconnect tip + Serve perf",
                                "Query load pack",
                                "Script verify forks + Class A commit",
                                "Header to record + Inbound eviction",
                                "Pin chain view",
                                "Tip future check + Unspendable script",
                                "TxOut size + Truncated median",
                                "Percentiles by weight",
                                "Work comparison + Bad prev error",
                                "Header timeout + Stale follow evict",
                                "Witness commitment",
                                "DNS seed query + Regtest candidate",
                                "Last height + Seal subscribe",
                                "Script hash",
                                "Soft densify + Esplora script + Esplora perf",
                                "Peer constants + Peer logs",
                                "More constants",
                                "Key & Address generation",
                                "BIP32 HD Wallet derivation",
                                "PSBT parse + extract",
                                "Chain constants + Chain logs",
                                "V2 transport constants",
                                "More constants 2",
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
