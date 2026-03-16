from mrjob.job import MRJob

class EscrutinioNacional(MRJob):

    def mapper(self, _, linea):
        voto = linea.strip()
        # INICIA TU CODIGO AQUI
        # Usa la palabra reservada 'yield' para emitir la pareja (Clave, Valor)
        # La Clave es la variable 'voto', el Valor es el numero 1
        yield voto, 1
        # TERMINA TU CODIGO AQUI

    def reducer(self, candidato, conteos):
        # INICIA TU CODIGO AQUI
        # 'conteos' es un generador con todos los 1s juntos: [1, 1, 1...]
        # Calcula la suma y emite la Clave (candidato) y el Valor final (la suma)
        total = sum(conteos) # Pista: usa sum()
        yield candidato, total
        # TERMINA TU CODIGO AQUI

if __name__ == '__main__':
    EscrutinioNacional.run()
