// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { Script } from "forge-std/Script.sol";
import { console2 } from "forge-std/console2.sol";

import { Hooks } from "v4-core/libraries/Hooks.sol";
import { IPoolManager } from "v4-core/interfaces/IPoolManager.sol";

import { OracleAnchoredLVRHook } from "src/OracleAnchoredLVRHook.sol";
import { HookMiner } from "./utils/HookMiner.sol";

/// @notice Mines a permission-encoded address and deploys OracleAnchoredLVRHook
/// through the deterministic CREATE2 deployer proxy.
///
/// Usage (see docs/deployment.md):
///   forge script script/DeployHook.s.sol:DeployHook \
///     --rpc-url base_sepolia --private-key $DEPLOYER_KEY --broadcast
///
/// Environment:
///   POOL_MANAGER  optional override; defaults per chain id below.
///   HOOK_OWNER    optional; defaults to the broadcast sender. The hook has no
///                 ownership transfer, so this address permanently controls
///                 setConfig / setRiskState.
contract DeployHook is Script {
    /// @dev Forge routes salted `new` in broadcast mode through this proxy on all
    /// target chains, so the mined address must be computed against it.
    address internal constant CREATE2_DEPLOYER = 0x4e59b44847b379578588920cA78FbF26c0B4956C;

    address internal constant BASE_SEPOLIA_POOL_MANAGER =
        0x05E73354cFDd6745C338b50BcFDfA3Aa6fA03408;
    address internal constant UNICHAIN_SEPOLIA_POOL_MANAGER =
        0x00B036B58a818B1BC34d502D3fE730Db729e62AC;

    function run() external returns (OracleAnchoredLVRHook hook) {
        IPoolManager poolManager = IPoolManager(vm.envOr("POOL_MANAGER", _defaultPoolManager()));
        address hookOwner = vm.envOr("HOOK_OWNER", msg.sender);
        require(
            hookOwner != DEFAULT_SENDER,
            "DeployHook: set HOOK_OWNER or pass --private-key/--sender; the forge default "
            "sender would own the hook forever"
        );

        uint160 flags = uint160(Hooks.BEFORE_ADD_LIQUIDITY_FLAG | Hooks.BEFORE_SWAP_FLAG);
        (address minedAddress, bytes32 salt) = HookMiner.find(
            CREATE2_DEPLOYER,
            flags,
            type(OracleAnchoredLVRHook).creationCode,
            abi.encode(poolManager, hookOwner)
        );

        vm.startBroadcast();
        hook = new OracleAnchoredLVRHook{ salt: salt }(poolManager, hookOwner);
        vm.stopBroadcast();

        require(address(hook) == minedAddress, "DeployHook: deployed address mismatch");
        require(
            uint160(address(hook)) & HookMiner.FLAG_MASK == flags,
            "DeployHook: permission bits mismatch"
        );

        console2.log("OracleAnchoredLVRHook:", address(hook));
        console2.log("  pool manager:       ", address(poolManager));
        console2.log("  owner:              ", hookOwner);
        console2.log("  salt:               ", vm.toString(salt));
    }

    function _defaultPoolManager() internal view returns (address) {
        if (block.chainid == 84_532) return BASE_SEPOLIA_POOL_MANAGER;
        if (block.chainid == 1301) return UNICHAIN_SEPOLIA_POOL_MANAGER;
        revert("DeployHook: no default PoolManager for this chain; set POOL_MANAGER");
    }
}
