// SPDX-License-Identifier: MIT
pragma solidity 0.8.26;

/// @notice Specification sketch only. Not audited, not deployed.
interface IDutchAuctionModule {
    struct AuctionConfig {
        uint16 startConcessionBps;
        uint16 concessionGrowthBpsPerSecond;
        uint16 maxConcessionBps;
        uint32 maxAuctionDurationSeconds;
        uint256 solverGasCostQuote;
        uint16 solverEdgeBps;
        uint256 minAuctionStaleLossQuote;
        bytes32 triggerMode;
        bytes32 reserveMode;
        uint16 reserveHookMarginBps;
        uint256 minLpUpliftQuote;
        uint16 minLpUpliftStaleLossBps;
        uint16 solverPaymentHookCapMultipleBps;
    }

    struct AuctionView {
        bytes32 poolId;
        bytes32 swapHash;
        uint256 exactStaleLossQuote;
        uint256 hookFeeRevenueQuote;
        uint256 solverRequiredQuote;
        uint256 deadline;
        uint16 currentConcessionBps;
        bool oracleFresh;
        bool settled;
    }

    event AuctionOpened(
        bytes32 indexed poolId,
        bytes32 indexed swapHash,
        uint256 exactStaleLossQuote,
        uint16 startConcessionBps,
        uint256 deadline
    );
    event AuctionFilled(bytes32 indexed swapHash, address indexed solver, uint256 paymentQuote);
    event AuctionCancelled(bytes32 indexed swapHash, bytes32 reason);
    event AuctionConfigSet(bytes32 indexed poolId, AuctionConfig config);

    function fillAuction(bytes32 swapHash, uint256 paymentQuote) external;
    function getAuction(bytes32 swapHash) external view returns (AuctionView memory auction);
    function setAuctionConfig(bytes32 poolId, AuctionConfig calldata config) external;
    function cancelAuction(bytes32 swapHash, bytes32 reason) external;
}
