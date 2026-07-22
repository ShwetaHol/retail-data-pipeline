const sources = ["orders", "customers", "order_items", "products", "order_payments"];

sources.forEach(name => {
  declare({
    database: "retail-pipeline-502300",
    schema: "raw",
    name: name
  });
});