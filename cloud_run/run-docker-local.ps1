# Build da imagem
docker build -f Dockerfile.local -t instagram-test-local .

# Rodar com Excel montado como volume
docker run --rm `
  -v "${PWD}\..\Perfis testes - Novembro.xlsx:/app/excel/Perfis_testes_Novembro.xlsx" `
  -v "${PWD}\json_local:/app/json_local" `
  -v "${PWD}\csv_local:/app/csv_local" `
  -e INSTAGRAM_LOGIN="testdevjg" `
  -e INSTAGRAM_SENHA="Evalleiford10" `
  -e EXCEL_PATH="/app/excel/Perfis_testes_Novembro.xlsx" `
  -e HEADLESS="true" `
  -e DELAY="5" `
  instagram-test-local

# Ver logs
docker logs instagram-test-local
