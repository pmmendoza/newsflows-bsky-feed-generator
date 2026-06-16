FROM node:18-slim AS prod-deps

WORKDIR /app

COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile --production=true \
  && yarn licenses generate-disclaimer --production > THIRD_PARTY_NOTICES.md \
  && yarn cache clean

FROM node:18-slim AS build

WORKDIR /app

COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile

COPY tsconfig.json ./
COPY src ./src
RUN yarn build

FROM node:18-slim

WORKDIR /app

ENV NODE_ENV=production \
  FEEDGEN_LISTENHOST=0.0.0.0

COPY --from=prod-deps /app/node_modules ./node_modules
COPY --from=prod-deps /app/THIRD_PARTY_NOTICES.md ./THIRD_PARTY_NOTICES.md
COPY --from=build /app/dist ./dist
COPY package.json yarn.lock ./
COPY LICENSE ./LICENSE

# Keep published runtime images free of dependency test fixture env files.
RUN find ./node_modules -type f \( -name ".env" -o -name "*.env" \) -delete

EXPOSE 3000

CMD ["node", "dist/index.js"]
