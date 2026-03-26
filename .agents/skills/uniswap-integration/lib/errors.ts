export function formatExecutionError(error: unknown): string {
  if (error instanceof Error) {
    if (error.message.includes('insufficient funds')) return 'Insufficient native token balance for gas.';
    if (error.message.includes('execution reverted')) return `Transaction reverted: ${error.message}`;
    if (error.message.includes('User rejected')) return 'Transaction rejected by user.';
    if (error.message.includes('INSUFFICIENT_OUTPUT_AMOUNT')) return 'Slippage exceeded: output amount too low.';
    if (error.message.includes('EXCESSIVE_INPUT_AMOUNT')) return 'Slippage exceeded: input amount too high.';
    if (error.message.includes('EXPIRED')) return 'Transaction deadline expired.';
    return error.message;
  }
  return String(error);
}
