# BTC Binary VWAP & Momentum Bot

Standalone Polymarket BTC Up/Down trading bot for 5- or 15-minute markets.
It evaluates VWAP, deviation, momentum and related filters before placing
orders. The strategy and risk settings are in `config.json`.

## Run locally

1. Use Python 3.11 or newer.
2. Install dependencies: `python -m pip install -r requirements.txt`.
3. Copy `.env.example` to `.env` and add the required wallet and CLOB
   credentials. Never commit `.env` or expose credentials.
4. Review `config.json`; start with simulation enabled.
5. Run `python main.py`.

The terminal UI is the default. The optional web dashboard should remain
private unless it is separately protected.

## Project docs

- [CONFIG.md](CONFIG.md): strategy and runtime configuration.
- [PROJECT_LOGIC.md](PROJECT_LOGIC.md): strategy and implementation details.
- [docs/README.md](docs/README.md): setup guide.

This repository contains the BTC trading service only. Portfolio and Perps
copying live in the separate `polymarket-following-bot` repository.

Trading involves risk of total loss. Use simulation first; no result is
guaranteed.
