# Single image, several roles. The ROLE env var decides whether a container is
# the scheduler, a worker, the admin API or the mock EHR — same binary, so
# there is no risk of the worker and the scheduler drifting apart.
FROM node:22-alpine AS build
WORKDIR /app

COPY package.json ./
RUN npm install --no-audit --no-fund

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

FROM node:22-alpine
WORKDIR /app
ENV NODE_ENV=production

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY --from=build /app/dist ./dist
# schema.sql / seed.sql are read at runtime by the create-if-absent migration.
COPY db ./db

# Containers get SIGTERM on `docker compose down`. Workers use it to hand back
# their leases, so tini is worth the 100kB to make sure the signal arrives.
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "dist/main.js"]
