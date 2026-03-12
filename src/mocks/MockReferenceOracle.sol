// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

import {IReferenceOracle} from "../interfaces/IReferenceOracle.sol";

contract MockReferenceOracle is IReferenceOracle {
    uint256 internal _priceWad;
    uint256 internal _updatedAt;

    function latestPriceWad() external view returns (uint256 priceWad, uint256 updatedAt) {
        return (_priceWad, _updatedAt);
    }

    function setPriceWad(uint256 priceWad_) external {
        _priceWad = priceWad_;
    }

    function setUpdatedAt(uint256 updatedAt_) external {
        _updatedAt = updatedAt_;
    }

    function setLatestPrice(uint256 priceWad_, uint256 updatedAt_) external {
        _priceWad = priceWad_;
        _updatedAt = updatedAt_;
    }
}
