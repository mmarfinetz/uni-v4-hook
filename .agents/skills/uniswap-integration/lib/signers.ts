/**
 * Signer backends for Uniswap execution scripts.
 *
 * Supported signer types:
 *   - privateKey  : raw private key via env var or --privateKey flag (dev only)
 *   - turnkey     : Turnkey TEE-backed signing via API (production recommended)
 *   - kms         : AWS KMS signing via @aws-sdk/client-kms (enterprise)
 */

import { createWalletClient, http, type Account, type WalletClient } from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { getChain, getRpcUrl } from './clients.js';
import type { SupportedChainId } from './types.js';

export type SignerType = 'privateKey' | 'turnkey' | 'kms';

export interface SignerOptions {
  signerType?: SignerType;
  // privateKey signer
  privateKey?: string;
  // turnkey signer
  turnkeyApiPublicKey?: string;
  turnkeyApiPrivateKey?: string;
  turnkeyOrganizationId?: string;
  turnkeyWalletAddress?: string; // the on-chain address managed by Turnkey
  // kms signer
  kmsKeyId?: string;         // AWS KMS key ID or ARN
  kmsRegion?: string;        // AWS region, defaults to AWS_REGION env var
  awsAccessKeyId?: string;
  awsSecretAccessKey?: string;
}

/**
 * Resolve signer type from options or environment variables.
 * Priority: explicit signerType arg > env UNISWAP_SIGNER_TYPE > 'privateKey'
 */
export function resolveSignerType(signerType?: SignerType): SignerType {
  if (signerType) return signerType;
  const env = process.env.UNISWAP_SIGNER_TYPE as SignerType | undefined;
  if (env === 'turnkey' || env === 'kms') return env;
  return 'privateKey';
}

/**
 * Build a viem WalletClient using the specified signer backend.
 */
export async function getSignerWalletClient(
  chainId: SupportedChainId,
  opts: SignerOptions
): Promise<{ walletClient: WalletClient; address: `0x${string}` }> {
  const signerType = resolveSignerType(opts.signerType);

  if (signerType === 'turnkey') {
    return getTurnkeySigner(chainId, opts);
  }

  if (signerType === 'kms') {
    return getKmsSigner(chainId, opts);
  }

  // default: privateKey
  return getPrivateKeySigner(chainId, opts);
}

// ─── Private Key Signer ───────────────────────────────────────────────────────

async function getPrivateKeySigner(
  chainId: SupportedChainId,
  opts: SignerOptions
): Promise<{ walletClient: WalletClient; address: `0x${string}` }> {
  const pk = opts.privateKey ?? process.env.UNISWAP_EXEC_PRIVATE_KEY;
  if (!pk) {
    throw new Error(
      'Missing private key.\n' +
      'Set UNISWAP_EXEC_PRIVATE_KEY env var or pass --privateKey.\n' +
      'For production, use --signerType turnkey or --signerType kms instead.'
    );
  }
  const normalized = (pk.startsWith('0x') ? pk : `0x${pk}`) as `0x${string}`;
  const account = privateKeyToAccount(normalized);
  const walletClient = createWalletClient({
    chain: getChain(chainId),
    transport: http(getRpcUrl(chainId)),
    account,
  });
  return { walletClient, address: account.address };
}

// ─── Turnkey Signer ───────────────────────────────────────────────────────────

async function getTurnkeySigner(
  chainId: SupportedChainId,
  opts: SignerOptions
): Promise<{ walletClient: WalletClient; address: `0x${string}` }> {
  const apiPublicKey = opts.turnkeyApiPublicKey ?? process.env.TURNKEY_API_PUBLIC_KEY;
  const apiPrivateKey = opts.turnkeyApiPrivateKey ?? process.env.TURNKEY_API_PRIVATE_KEY;
  const organizationId = opts.turnkeyOrganizationId ?? process.env.TURNKEY_ORGANIZATION_ID;
  const walletAddress = opts.turnkeyWalletAddress ?? process.env.TURNKEY_WALLET_ADDRESS;

  if (!apiPublicKey || !apiPrivateKey || !organizationId || !walletAddress) {
    throw new Error(
      'Turnkey signer requires:\n' +
      '  TURNKEY_API_PUBLIC_KEY   - Turnkey API public key\n' +
      '  TURNKEY_API_PRIVATE_KEY  - Turnkey API private key\n' +
      '  TURNKEY_ORGANIZATION_ID  - Turnkey organization ID\n' +
      '  TURNKEY_WALLET_ADDRESS   - on-chain wallet address managed by Turnkey\n' +
      'Get these from https://app.turnkey.com'
    );
  }

  try {
    // Dynamic import — only required when using Turnkey
    const { Turnkey } = await import('@turnkey/sdk-server');
    const { createAccount } = await import('@turnkey/viem');

    const turnkeyClient = new Turnkey({
      apiBaseUrl: 'https://api.turnkey.com',
      apiPublicKey,
      apiPrivateKey,
      defaultOrganizationId: organizationId,
    });

    const account = await createAccount({
      client: turnkeyClient.apiClient(),
      organizationId,
      signWith: walletAddress,
      ethereumAddress: walletAddress as `0x${string}`,
    });

    const walletClient = createWalletClient({
      account,
      chain: getChain(chainId),
      transport: http(getRpcUrl(chainId)),
    });

    return { walletClient, address: walletAddress as `0x${string}` };
  } catch (err) {
    if (err instanceof Error && err.message.includes('Cannot find module')) {
      throw new Error(
        'Turnkey packages not installed. Run:\n' +
        '  npm install @turnkey/sdk-server @turnkey/viem'
      );
    }
    throw err;
  }
}

// ─── AWS KMS Signer ───────────────────────────────────────────────────────────

async function getKmsSigner(
  chainId: SupportedChainId,
  opts: SignerOptions
): Promise<{ walletClient: WalletClient; address: `0x${string}` }> {
  const keyId = opts.kmsKeyId ?? process.env.AWS_KMS_KEY_ID;
  const region = opts.kmsRegion ?? process.env.AWS_REGION ?? 'us-east-1';

  if (!keyId) {
    throw new Error(
      'AWS KMS signer requires:\n' +
      '  AWS_KMS_KEY_ID           - KMS key ID or ARN (ECC_SECG_P256K1 key)\n' +
      '  AWS_REGION               - AWS region (default: us-east-1)\n' +
      '  AWS_ACCESS_KEY_ID        - AWS access key (or use instance profile)\n' +
      '  AWS_SECRET_ACCESS_KEY    - AWS secret key (or use instance profile)\n' +
      'See: https://docs.aws.amazon.com/kms/latest/developerguide/create-keys.html'
    );
  }

  try {
    // Dynamic import — only required when using KMS
    const { KMSClient } = await import('@aws-sdk/client-kms');
    const { KMSSigner } = await import('@uniswap-ai/kms-signer');

    const kmsClient = new KMSClient({
      region,
      ...(opts.awsAccessKeyId && opts.awsSecretAccessKey
        ? {
            credentials: {
              accessKeyId: opts.awsAccessKeyId,
              secretAccessKey: opts.awsSecretAccessKey,
            },
          }
        : {}),
    });

    const kmsSigner = new KMSSigner(kmsClient, keyId);
    const address = await kmsSigner.getAddress();

    const account: Account = {
      address: address as `0x${string}`,
      type: 'local',
      signMessage: ({ message }) => kmsSigner.signMessage(message as string),
      signTransaction: (tx) => kmsSigner.signTransaction(tx as never),
      signTypedData: (data) => kmsSigner.signTypedData(data as never),
      source: 'custom',
      publicKey: '0x',
    };

    const walletClient = createWalletClient({
      account,
      chain: getChain(chainId),
      transport: http(getRpcUrl(chainId)),
    });

    return { walletClient, address: address as `0x${string}` };
  } catch (err) {
    if (err instanceof Error && err.message.includes('Cannot find module')) {
      throw new Error(
        'AWS KMS packages not installed. Run:\n' +
        '  npm install @aws-sdk/client-kms\n' +
        'Note: @uniswap-ai/kms-signer is a lightweight viem adapter included in this package.'
      );
    }
    throw err;
  }
}
