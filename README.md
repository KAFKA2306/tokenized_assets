# Tokenized Assets Primary Evidence

[![Tokenized assets evidence](https://github.com/KAFKA2306/tokenized_assets/actions/workflows/tokenized-assets.yml/badge.svg)](https://github.com/KAFKA2306/tokenized_assets/actions/workflows/tokenized-assets.yml)
[![Deploy Pages](https://github.com/KAFKA2306/tokenized_assets/actions/workflows/pages.yml/badge.svg)](https://github.com/KAFKA2306/tokenized_assets/actions/workflows/pages.yml)

Tokenized assetを**発行体の一次開示・法的asset identity・Ethereum上のtoken deployment・block-level evidence**へ分離し、raw evidenceから再生成可能なdatasetとして保存します。`api/v1/tokenized-assets/` が正準成果物です。

## Public dashboard

- Daily entry point: https://kafka2306.github.io/tokenized_assets/
- latest issuer-reported USDC circulation and reserve fair value
- latest Ethereum native USDC `totalSupply()` and week-over-week change
- issuer all-chain scopeとEthereum-only scopeを別laneで表示
- BUIDL / OUSGのverified Ethereum deployments
- mint/burn coverageとlatest observed event timestamp

Pagesはissuer factとchain factを同じcurrent valueへ補正しません。最新mint/burn eventがUTC日の途中ならdaily net issuanceとも表示しません。

## Canonical data

- [dataset index](api/v1/tokenized-assets/index.json)
- [asset / deployment registry](api/v1/tokenized-assets/registry.json)
- [USDC issuer observations](api/v1/tokenized-assets/issuer.json)
- [USDC Ethereum weekly supply](api/v1/tokenized-assets/chain-weekly.json)
- [current deployment snapshots](api/v1/tokenized-assets/deployments.json)
- [USDC mint / burn evidence](api/v1/tokenized-assets/mint-burn.json)
- [SEC filing ledger](api/v1/tokenized-assets/filings.json)
- [issuer ↔ chain reconciliation](api/v1/tokenized-assets/reconciliation.json)
- [raw provenance manifest](api/v1/tokenized-assets/provenance.json)

`Tokenized assets evidence` workflowが一次情報を取得し、raw response / issuer documentをSHA-256で固定した後、同じevidenceからAPIを生成します。CIでは保存済みevidenceだけでoffline再生成し、live生成物との差分がないことを検証します。

## USDC: issuer factとchain factを混ぜない

Circle reserve reportのissuer observationでは、all approved blockchainsを対象とするUSDC circulationとreserve fair valueを別fieldで保持します。

Ethereum側ではCircleが公開するnative USDC contractについて、finalized Ethereum blockを基準に週次`totalSupply()`を観測します。各recordはchain ID、block number/hash、contract address、observed_at、total supplyを保持します。

issuer-reported all-chain circulationとEthereum native `totalSupply()`はscopeが異なるため、同じ値として補正しません。`reconciliation.json`には観測差をそのまま残し、guessed correctionを適用しません。

## Mint / burn

USDCのsupply-changing eventだけをEthereum `Transfer` logから抽出します。

- `from == 0x0` → mint
- `to == 0x0` → burn
- 通常のpeer-to-peer transfer → supplyを変えないため正準event ledgerには保存しない

各eventはblock number/hash、transaction hash、log indexを保持します。RPC providerはtransportであり、provenance authorityはEthereumのchain IDとblock hashです。

## Tokenized funds

BUIDL / OUSGはlegal assetとtoken deploymentを分けて登録します。複数official contractsは暗黙に1件へ統合しません。

## Data contract

- issuer-reported circulation / reserve fair value / Ethereum token supplyを別metricにする
- legal asset identity / token deployment identityを分離する
- Ethereum mainnet以外、missing code、取得不能ERC-20 state、raw hash mismatchはfail closed
- mint/burnと普通のtransferを混ぜない
- reconciliationは観測差を保持し、推測補正しない
- source evidenceからderived APIを再生成できる

## Verification

```bash
python tokenized_assets.py
python tokenized_assets.py --offline
python -m unittest discover -v
```

- `Tokenized assets evidence` はissuer / SEC / Ethereum一次証拠とoffline rebuildを検証します。
- `Deploy Pages` はPRでscope separationとdashboard JSを検証し、mainではpublic projectionをdeployしてexact SHA・issuer scope・chain IDを照合します。

Tracking issue: https://github.com/KAFKA2306/tokenized_assets/issues/11
