class Pgsalesgen < Formula
  include Language::Python::Virtualenv

  desc "PostgreSQL Sales Generator"
  homepage "https://github.com/rioriost/homebrew-pgsalesgen/"
  url "https://files.pythonhosted.org/packages/2d/73/de5c21595b86c3e0abd1a110977a26ab5475144d2f52700fee9404cabca4/pgsalesgen-0.1.0.tar.gz"
  sha256 "6b29e1346923261975009abb447a7bcb5f6172db759bf6f9b22c2fbd090afb8e"
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
    system "#{bin}/pg-salesgen", "--help"
  end
end
