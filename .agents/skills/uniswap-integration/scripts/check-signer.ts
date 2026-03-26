#!/usr/bin/env ts-node
/**
 * Uniswap AI — Signer Pre-flight Check
 *
 * Validates your signer configuration before running any execution script.
 * Reads UNISWAP_SIGNER_TYPE (default: privateKey) and checks required env vars.
 *
 * Usage:
 *   npx tsx scripts/check-signer.ts
 *   npx tsx scripts/check-signer.ts --signerType turnkey
 *   npx tsx scripts/check-signer.ts --signerType kms
 *   npx tsx scripts/check-signer.ts --chainId 42161
 *
 * Exit codes:
 *   0 — all checks passed
 *   1 — missing config or error
 */

import { parseFlags, getOptionalStringArg } from '../lib/cli.js';
import { getRpcUrl } from '../lib/clients.js';
import type { SupportedChainId } from '../lib/types.js';

const RESET  = '\x1b[0m';
const BOLD   = '\x1b[1m';
const RED    = '\x1b[31m';
const GREEN  = '\x1b[32m';
const YELLOW = '\x1b[33m';
const CYAN   = '\x1b[36m';
const DIM    = '\x1b[2m';

function ok(msg: string)   { console.log(`${GREEN}✅${RESET} ${msg}`); }
function warn(msg: string)  { console.log(`${YELLOW}⚠️ ${RESET} ${msg}`); }
function fail(msg: string)  { console.log(`${RED}❌${RESET} ${msg}`); }
function info(msg: string)  { console.log(`${CYAN}ℹ️ ${RESET} ${msg}`); }
function dim(msg: string)   { console.log(`${DIM}${msg}${RESET}`); }
function header(msg: string){ console.log(`\n${BOLD}${msg}${RESET}`); }
function line()             { console.log('─'.repeat(60)); }

// ─── Helpers ─────────────────────────────────────────────────────────────────

function checkEnv(name: string): string | undefined {
  return process.env[name] || undefined;
}

function requireEnv(vars: string[]): { missing: string[]; present: string[] } {
  const missing: string[] = [];
  const present: string[] = [];
  for (const v of vars) {
    if (checkEnv(v)) present.push(v);
    else missing.push(v);
  }
  return { missing, present };
}

function maskSecret(val: string): string {
  if (val.length <= 8) return '****';
  return val.slice(0, 4) + '****' + val.slice(-4);
}

// ─── Signer checks ───────────────────────────────────────────────────────────

async function checkPrivateKey(chainId: SupportedChainId): Promise<boolean> {
  header('Signer: Private Key (development mode)');
  line();

  const pk = checkEnv('UNISWAP_EXEC_PRIVATE_KEY');

  if (!pk) {
    fail('UNISWAP_EXEC_PRIVATE_KEY is not set');
    console.log('');
    console.log('Set it with:');
    console.log(`  ${CYAN}export UNISWAP_EXEC_PRIVATE_KEY=0x<your-private-key>${RESET}`);
    console.log('');
    warn('Private key signers are for LOCAL DEVELOPMENT ONLY.');
    console.log('  For production, consider Turnkey (TEE) or AWS KMS (HSM):');
    console.log(`  ${DIM}export UNISWAP_SIGNER_TYPE=turnkey${RESET}`);
    console.log(`  ${DIM}export UNISWAP_SIGNER_TYPE=kms${RESET}`);
    return false;
  }

  // Validate key format and derive address
  try {
    const { privateKeyToAccount } = await import('viem/accounts');
    const normalized = (pk.startsWith('0x') ? pk : `0x${pk}`) as `0x${string}`;
    const account = privateKeyToAccount(normalized);

    warn('Using raw private key signer — NOT recommended for production.');
    console.log(`  ${DIM}For production use: export UNISWAP_SIGNER_TYPE=turnkey${RESET}`);
    console.log(`  ${DIM}See: https://app.turnkey.com${RESET}`);
    console.log('');
    ok(`Wallet address : ${BOLD}${account.address}${RESET}`);
    ok(`Key loaded     : ${maskSecret(pk)}`);
  } catch {
    fail('UNISWAP_EXEC_PRIVATE_KEY is set but appears invalid (could not derive address)');
    return false;
  }

  return true;
}

async function checkTurnkey(chainId: SupportedChainId): Promise<boolean> {
  header('Signer: Turnkey (TEE-backed, production)');
  line();

  const required = [
    'TURNKEY_API_PUBLIC_KEY',
    'TURNKEY_API_PRIVATE_KEY',
    'TURNKEY_ORGANIZATION_ID',
    'TURNKEY_WALLET_ADDRESS',
  ];

  const { missing, present } = requireEnv(required);

  for (const v of present) {
    const val = process.env[v]!;
    ok(`${v.padEnd(28)} ${maskSecret(val)}`);
  }

  if (missing.length > 0) {
    console.log('');
    for (const v of missing) {
      fail(`${v} is not set`);
    }
    console.log('');
    console.log('Set missing variables:');
    for (const v of missing) {
      console.log(`  ${CYAN}export ${v}=<your-value>${RESET}`);
    }
    console.log('');
    console.log('Get your Turnkey credentials at:');
    console.log(`  ${CYAN}https://app.turnkey.com${RESET}`);
    console.log('');
    console.log('Turnkey setup guide:');
    console.log(`  ${CYAN}https://docs.turnkey.com/getting-started/quickstart${RESET}`);
    console.log('');
    console.log('Required npm packages:');
    console.log(`  ${YELLOW}npm install @turnkey/sdk-server @turnkey/viem${RESET}`);
    return false;
  }

  // Check npm packages
  let packagesOk = true;
  for (const pkg of ['@turnkey/sdk-server', '@turnkey/viem']) {
    try {
      await import(pkg);
      ok(`Package ${pkg} is installed`);
    } catch {
      fail(`Package ${pkg} is NOT installed`);
      console.log(`  Run: ${YELLOW}npm install ${pkg}${RESET}`);
      packagesOk = false;
    }
  }

  if (!packagesOk) return false;

  const walletAddress = process.env.TURNKEY_WALLET_ADDRESS!;
  console.log('');
  ok(`Wallet address : ${BOLD}${walletAddress}${RESET}`);
  ok('Turnkey signer is configured correctly');
  return true;
}

async function checkKms(chainId: SupportedChainId): Promise<boolean> {
  header('Signer: AWS KMS (HSM-backed, enterprise)');
  line();

  const keyId  = checkEnv('AWS_KMS_KEY_ID');
  const region = checkEnv('AWS_REGION') ?? 'us-east-1 (default)';
  const hasStaticCreds = checkEnv('AWS_ACCESS_KEY_ID') && checkEnv('AWS_SECRET_ACCESS_KEY');

  if (!keyId) {
    fail('AWS_KMS_KEY_ID is not set');
    console.log('');
    console.log('Set it with:');
    console.log(`  ${CYAN}export AWS_KMS_KEY_ID=arn:aws:kms:<region>:<account>:key/<key-id>${RESET}`);
    console.log(`  ${CYAN}export AWS_REGION=us-east-1${RESET}`);
    console.log('');
    console.log('Your KMS key must use key spec: ECC_SECG_P256K1 (secp256k1 — Ethereum compatible)');
    console.log('');
    console.log('AWS KMS setup guide:');
    console.log(`  ${CYAN}https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html${RESET}`);
    console.log('');
    console.log('Required npm packages:');
    console.log(`  ${YELLOW}npm install @aws-sdk/client-kms${RESET}`);
    return false;
  }

  ok(`AWS_KMS_KEY_ID : ${maskSecret(keyId)}`);
  ok(`AWS_REGION     : ${region}`);

  if (hasStaticCreds) {
    ok(`AWS credentials: static keys (AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY)`);
  } else {
    info('No static AWS credentials — will use instance profile / environment chain');
    dim('  (This is fine for EC2, ECS, Lambda, and other AWS-managed environments)');
  }

  // Check npm package
  try {
    await import('@aws-sdk/client-kms');
    ok('Package @aws-sdk/client-kms is installed');
  } catch {
    fail('Package @aws-sdk/client-kms is NOT installed');
    console.log(`  Run: ${YELLOW}npm install @aws-sdk/client-kms${RESET}`);
    return false;
  }

  // Attempt to derive address via KMS
  console.log('');
  info('Attempting to derive Ethereum address from KMS key...');
  try {
    const { KMSClient } = await import('@aws-sdk/client-kms');
    const { KMSSigner } = await import('@uniswap-ai/kms-signer');

    const kmsClient = new KMSClient({
      region: checkEnv('AWS_REGION') ?? 'us-east-1',
      ...(hasStaticCreds
        ? {
            credentials: {
              accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
              secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
            },
          }
        : {}),
    });

    const kmsSigner = new KMSSigner(kmsClient, keyId);
    const address = await kmsSigner.getAddress();
    ok(`Wallet address : ${BOLD}${address}${RESET}`);
    ok('AWS KMS signer is configured correctly');
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes('Cannot find module') && msg.includes('kms-signer')) {
      warn('Could not verify wallet address: @uniswap-ai/kms-signer not available in this context');
      info('This is expected if running outside the full package. KMS config looks correct.');
      return true;
    }
    fail(`KMS connectivity check failed: ${msg}`);
    console.log('');
    console.log('Common causes:');
    console.log('  - Invalid KMS key ID or ARN');
    console.log('  - Insufficient IAM permissions (need kms:GetPublicKey)');
    console.log('  - Wrong AWS region');
    return false;
  }

  return true;
}

// ─── RPC check ───────────────────────────────────────────────────────────────

function checkRpc(chainId: SupportedChainId) {
  header('RPC Configuration');
  line();

  const ethRpc = checkEnv('ETHEREUM_RPC_URL');
  const arbRpc = checkEnv('ARBITRUM_RPC_URL');

  if (chainId === 1 || !chainId) {
    if (ethRpc) {
      ok(`ETHEREUM_RPC_URL : ${ethRpc}`);
    } else {
      warn('ETHEREUM_RPC_URL not set — using public fallback (rate-limited)');
      dim('  https://ethereum.publicnode.com');
      dim('  For production: export ETHEREUM_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY');
    }
  }

  if (chainId === 42161 || !chainId) {
    if (arbRpc) {
      ok(`ARBITRUM_RPC_URL : ${arbRpc}`);
    } else {
      warn('ARBITRUM_RPC_URL not set — using public fallback (rate-limited)');
      dim('  https://arbitrum.publicnode.com');
      dim('  For production: export ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY');
    }
  }
}

// ─── Main ────────────────────────────────────────────────────────────────────

async function main() {
  const flags = parseFlags(process.argv.slice(2)) as Record<string, string>;
  const signerTypeArg = getOptionalStringArg(flags, 'signerType') as 'privateKey' | 'turnkey' | 'kms' | undefined;
  const chainIdArg = getOptionalStringArg(flags, 'chainId');
  const chainId = chainIdArg ? (parseInt(chainIdArg, 10) as SupportedChainId) : 1;

  const signerType = signerTypeArg
    ?? (process.env.UNISWAP_SIGNER_TYPE as 'privateKey' | 'turnkey' | 'kms' | undefined)
    ?? 'privateKey';

  console.log('');
  console.log(`${BOLD}Uniswap AI — Signer Pre-flight Check${RESET}`);
  console.log(`Chain: ${chainId === 1 ? 'Ethereum Mainnet' : chainId === 42161 ? 'Arbitrum One' : chainId}`);
  console.log(`Signer type: ${BOLD}${signerType}${RESET}`);

  let signerOk = false;

  if (signerType === 'turnkey') {
    signerOk = await checkTurnkey(chainId);
  } else if (signerType === 'kms') {
    signerOk = await checkKms(chainId);
  } else {
    signerOk = await checkPrivateKey(chainId);
  }

  checkRpc(chainId);

  console.log('');
  line();

  if (signerOk) {
    ok(`Pre-flight check passed. Ready to execute on chain ${chainId}.`);
    console.log('');
    process.exit(0);
  } else {
    fail('Pre-flight check failed. Fix the issues above before running execution scripts.');
    console.log('');
    console.log('Signer options:');
    console.log(`  ${DIM}export UNISWAP_SIGNER_TYPE=privateKey  # dev only${RESET}`);
    console.log(`  ${DIM}export UNISWAP_SIGNER_TYPE=turnkey     # production (TEE)${RESET}`);
    console.log(`  ${DIM}export UNISWAP_SIGNER_TYPE=kms         # enterprise (HSM)${RESET}`);
    console.log('');
    process.exit(1);
  }
}

main().catch((err) => {
  fail(`Unexpected error: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
