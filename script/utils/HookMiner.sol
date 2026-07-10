// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import { Hooks } from "v4-core/libraries/Hooks.sol";

/// @notice Mines a CREATE2 salt so the hook deploys to an address whose low 14
/// bits encode exactly its permission flags, as `Hooks.isValidHookAddress`
/// requires. The vendored v4-periphery predates the upstream HookMiner utility,
/// so this is a minimal local equivalent.
library HookMiner {
    uint160 internal constant FLAG_MASK = Hooks.ALL_HOOK_MASK;
    uint256 internal constant MAX_LOOP = 200_000;

    error NoSaltFound(uint160 flags);

    /// @param deployer The CREATE2 deployer that will run the deployment. For a
    /// broadcast `new Contract{salt: salt}(...)` in a forge script this is the
    /// deterministic deployer proxy, not the EOA sending the transaction.
    function find(
        address deployer,
        uint160 flags,
        bytes memory creationCode,
        bytes memory constructorArgs
    ) internal view returns (address hookAddress, bytes32 salt) {
        flags = flags & FLAG_MASK;
        bytes32 initCodeHash = keccak256(abi.encodePacked(creationCode, constructorArgs));

        for (uint256 i = 0; i < MAX_LOOP; ++i) {
            hookAddress = computeAddress(deployer, bytes32(i), initCodeHash);
            if (uint160(hookAddress) & FLAG_MASK == flags && hookAddress.code.length == 0) {
                return (hookAddress, bytes32(i));
            }
        }
        revert NoSaltFound(flags);
    }

    function computeAddress(address deployer, bytes32 salt, bytes32 initCodeHash)
        internal
        pure
        returns (address)
    {
        return address(
            uint160(
                uint256(keccak256(abi.encodePacked(bytes1(0xff), deployer, salt, initCodeHash)))
            )
        );
    }
}
