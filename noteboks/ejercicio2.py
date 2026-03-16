from mrjob.job import MRJob

class EscrutinioLimpio(MRJob):

    def mapper(self, _, linea):
        voto = linea.strip()
        candidatos_oficiales = ['Cepeda', 'Paloma', 'Fajardo', 'Abelardo']

        # INICIA TU CODIGO AQUI
        # Pon un 'if' para asegurar que el 'voto' esta dentro de 'candidatos_oficiales'
        # Solo si se cumple, haz el yield.
        if voto in candidatos_oficiales:
            yield voto, 1
        pass
        # TERMINA TU CODIGO AQUI

    def reducer(self, candidato, conteos):
        yield candidato, sum(conteos)

if __name__ == '__main__':
    EscrutinioLimpio.run()
