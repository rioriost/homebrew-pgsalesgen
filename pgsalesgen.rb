class Pgsalesgen < Formula
  include Language::Python::Virtualenv

  desc "PostgreSQL Sales Generator"
  homepage "https://github.com/rioriost/homebrew-pgsalesgen/"
  url "https://files.pythonhosted.org/packages/e2/a3/a9d4fc196aa69402fba9134f90ac0a245eb447bbce1d5ee560829e3ddc9c/pgsalesgen-0.1.1.tar.gz"
  sha256 "c70f2460fcc36587e951f7ee4b38170018827b1482f7e39ad2c88ab5d42f9abe"
  license "MIT"

  depends_on "python@3.14"
  depends_on "libpq"

  def install
    venv = virtualenv_create(libexec, "python3.14")

    ENV["PG_CONFIG"] = Formula["libpq"].opt_bin/"pg_config"

    venv.pip_install_and_link buildpath

    venv.pip_install "psycopg[c]"
  end

  test do
    system "#{bin}/pgsalesgen", "--help"
  end
end
