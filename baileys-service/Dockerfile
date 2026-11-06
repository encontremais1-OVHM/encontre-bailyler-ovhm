FROM node:20-bookworm-slim AS build
WORKDIR /app

# Baileys 6.7.x pulls libsignal from a Git repository during npm install.
# The slim Node image does not include Git, so install it in the build stage.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install --omit=optional --no-audit --no-fund

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:20-bookworm-slim AS runtime
ENV NODE_ENV=production
WORKDIR /app

COPY package*.json ./
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist

RUN mkdir -p /data/baileys-auth && chown -R node:node /data
USER node

EXPOSE 3000
CMD ["node", "dist/index.js"]
