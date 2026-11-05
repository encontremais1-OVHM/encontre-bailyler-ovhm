FROM node:20-bookworm-slim AS build
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=optional
COPY tsconfig.json ./
COPY src ./src
RUN npm install --no-save typescript && npm run build

FROM node:20-bookworm-slim
ENV NODE_ENV=production
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev --omit=optional
COPY --from=build /app/dist ./dist
RUN mkdir -p /data/baileys-auth && chown -R node:node /data
USER node
EXPOSE 3000
CMD ["node", "dist/index.js"]
