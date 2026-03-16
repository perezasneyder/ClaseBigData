from mrjob.job import MRJob

class MaximoPorCandidato(MRJob):

    def mapper(self, _, linea):
        partes = linea.strip().split(',')
        if len(partes) == 3:
            candidato = partes[0]
            votos = int(partes[2])  # Cuidado: convertir texto a entero

            # INICIA TU CODIGO AQUI
            # Emite el candidato y los votos de esta acta especifica
            yield candidato, votos
            # TERMINA TU CODIGO AQUI

    def reducer(self, candidato, votos_de_todas_las_mesas):
        # INICIA TU CODIGO AQUI
        # Usa la funcion estadistica max() para iterar y sacar el maximo
        record = max(votos_de_todas_las_mesas)
        yield candidato, record
        # TERMINA TU CODIGO AQUI

if __name__ == '__main__':
    MaximoPorCandidato.run()
