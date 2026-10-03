# The SDK matrix's JavaScript client (tests/sdk/run.sh). The package
# versions are pinned by package-lock.json.
FROM node:22.23.3-bookworm-slim@sha256:43ac6c60b8f89723f746e8a92ce91abd5017e627ce1ddfe4238355d3a30b772c
WORKDIR /client
COPY package.json package-lock.json ./
RUN npm ci --ignore-scripts --no-audit --no-fund
COPY sdk-test.mjs .
ENTRYPOINT ["node", "/client/sdk-test.mjs"]
