// SPDX-License-Identifier: MIT
pragma solidity ^0.8.26;

import {IReferenceOracle} from "../../src/interfaces/IReferenceOracle.sol";

contract MockReferenceOracle is IReferenceOracle {
    uint256 private _priceWad;
    uint256 private _updatedAt;

    function setPrice(uint256 priceWad, uint256 updatedAt) external {
        _priceWad = priceWad;
        _updatedAt = updatedAt;
    }

    function latestPriceWad() external view returns (uint256 priceWad, uint256 updatedAt) {
        return (_priceWad, _updatedAt);
    }
}
