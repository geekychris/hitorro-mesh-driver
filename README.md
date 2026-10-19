<p align="center">
  <img src="assets/logo.png" alt="Hitorro Mesh Driver logo" width="180">
</p>

# Hitorro Mesh Driver

Query planner and dispatcher for Hitorro Mesh — takes a SQL query, plans it against distributed JVS tables with pushdown/shuffle stages, dispatches per-partition tasks to agents over MeshTransport, and streams result rows back to the client. Live agent registry, placement, and shuffle live here.
