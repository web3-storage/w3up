import { Failure, Result } from '@ucanto/interface'
import {
  ProviderDID,
  SpaceDID,
  UsageData,
} from '@web3-storage/capabilities/types'

export type { UsageData }

export interface UsageStorage {
  report: (
    provider: ProviderDID,
    space: SpaceDID,
    period: { from: Date; to: Date }
  ) => Promise<Result<UsageData, Failure>>
}
