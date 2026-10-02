const [origin, expectedName] = process.argv.slice(2);
if (!origin || expectedName === undefined) {
  throw new Error("usage: node queue_url_browser.mjs ORIGIN QUEUE_NAME");
}

const listResponse = await fetch(`${origin}/apps/fixture-app/queues`);
if (!listResponse.ok) {
  throw new Error(`queue list returned ${listResponse.status}`);
}
const listHTML = await listResponse.text();
const match = listHTML.match(
  /<a class="link queue-detail-link" href="([^"]+)"/,
);
if (!match) {
  throw new Error("served queue list did not contain a detail link");
}
const servedHref = match[1].replaceAll("&amp;", "&");
const resolved = new URL(servedHref, origin);
if (resolved.pathname !== "/apps/fixture-app/queue") {
  throw new Error(
    `WHATWG resolved ${servedHref} to unexpected path ${resolved.pathname}`,
  );
}
const queryKeys = [...resolved.searchParams.keys()];
if (
  queryKeys.length !== 1 ||
  queryKeys[0] !== "name" ||
  resolved.searchParams.get("name") !== expectedName
) {
  throw new Error(
    `WHATWG resolved ${servedHref} to unexpected query ${resolved.search}`,
  );
}

const detailResponse = await fetch(resolved);
if (!detailResponse.ok) {
  throw new Error(`resolved queue detail returned ${detailResponse.status}`);
}
const detailHTML = await detailResponse.text();
if (!detailHTML.includes(`data-field="name">${expectedName}</td>`)) {
  throw new Error("resolved queue detail did not render the exact queue name");
}
console.log(
  JSON.stringify({ servedHref, resolved: resolved.href, expectedName }),
);
