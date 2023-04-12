package common.curated

object DataIntegratorSetup {
  def setup =
    Vector(
      new SalesFactDataIntegrator(),
      new ProductDataIntegrator(),
      new CustomerDataIntegrator(),
      new ProductDataIntegrator()
    )
}
