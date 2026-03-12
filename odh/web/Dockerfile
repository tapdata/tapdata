# Build image
#  - Run command `docker build -t cloud/console:v1 .` in directory tapdata-enterprise-web
# Usage image
#  - Parameter USER_ID: Specify the identity of the user, all operations of the user on the console are performed by the identity of this user.
#  - Parameter SERVER_URI: Gateway server uri.
#  - Run command `docker run -it --rm -e SERVER_URI=http://172.31.119.19:30104 -e USER_ID=60c06045e18eaf41b9d1ff3e -p 18080:8080 cloud/console:v1` to start container.
#

FROM node:20

ENV NODE_ENV=community

# 1. Install pnpm globally using the mirror (cache this layer)
# We install the version specified in package.json to avoid lockfile incompatibility
RUN npm install -g pnpm@10.7.0 --registry=https://registry.npmmirror.com

WORKDIR /opt/web
COPY . /opt/web/

# 2. Install dependencies
# We split this from the previous step to identify where it hangs
# We explicitly set the registry for pnpm to ensure it uses the mirror
# We remove pnpm-lock.yaml to ensure fresh install of new dependencies
RUN pnpm clean
RUN pnpm i

# 3. Build the application
RUN pnpm build:daas

# 4. Prepare runtime environment for server.js
RUN mkdir -p /app && cd /app && npm install express http-proxy-middleware
# vite.config.ts outputs to ../../dist which is /opt/web/dist relative to /opt/web/apps/daas
RUN ln -s /opt/web/dist /app/dist

EXPOSE ${WEB_PORT:-3030}/tcp
ENV SERVE_ENV=PROD

# CMD pnpm --filter daas dev --mode community --port ${WEB_PORT:-8080} --host 0.0.0.0 -- -- --origin http://tm:${TM_PORT:-3030}
CMD node /app/server.js