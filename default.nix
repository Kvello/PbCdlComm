let
  nixpkgs = fetchTarball "https://github.com/NixOS/nixpkgs/tarball/nixos-25.05";
  pkgs = import nixpkgs { config = {}; overlays = []; };
in
with pkgs;

stdenv.mkDerivation {
  pname = "cpp-project";
  version = "0.1";
  src = ./.;
  buildInputs = [
    libxml2
    log4cpp
    cmake
    gcc
    gnumake42
    gdb
    pkg-config
  ];
  
  # Optional: if using Makefile
  # buildPhase = "make";
  # installPhase = "mkdir -p $out/bin && cp your_binary $out/bin/";


shellHook = ''
  mkdir -p .vscode


  # Get include paths from NIX_CFLAGS_COMPILE
  includes=$(echo $NIX_CFLAGS_COMPILE | tr ' ' '\n' | grep "^/nix" | sort -u)

  # Build JSON array lines
  include_json=$(printf '        "%s",\n' $includes)

  cat > .vscode/c_cpp_properties.json <<'EOF'
{
  "configurations": [
    {
      "name": "Nix",
      "compilerPath": "g++",
      "cStandard": "gnu99",
      "cppStandard": "c++03",
      "includePath": [
EOF

  # Now append the dynamically generated include paths
  echo "$include_json" >> .vscode/c_cpp_properties.json

  libxml_inc=$(pkg-config --cflags-only-I libxml-2.0 | sed 's/-I//g')
  echo "        \"$libxml_inc\"," >> .vscode/c_cpp_properties.json

  # Append the workspaceFolder entry literally
  cat >> .vscode/c_cpp_properties.json <<'EOF'
        "''${workspaceFolder}/**"
      ]
    }
  ],
  "version": 4
}
EOF
'';
}
