FROM node:20-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
COPY apps/api/package.json apps/api/package.json
COPY apps/consumer/package.json apps/consumer/package.json
COPY packages/core/package.json packages/core/package.json
RUN npm ci

FROM node:20-alpine AS runtime
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
WORKDIR /app/apps/consumer
USER node
CMD ["npx", "tsx", "src/index.ts"]
