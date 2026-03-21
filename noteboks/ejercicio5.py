from mrjob.job import MRJob

class PromedioActas(MRJob):

    def mapper(self, _, linea):
        partes = linea.strip().split(',')
        if len(partes) == 3:
            candidato = partes[0]
            votos = int(partes[2])

            # INICIA TU CODIGO AQUI
            # Para reconstruir el promedio, el Mapper debe entregar DOS valores como tupla: (la suma, el conteo)
            # Ej: Emite los votos leídos, y el numero 1 para ir contando de acta en acta.
            yield candidato, (votos, 1)
            # TERMINA TU CODIGO AQUI

    def reducer(self, candidato, valores_tuplas):
        suma_total = 0
        cantidad_mesas = 0

        # INICIA TU CODIGO AQUI
        # Desempacamos la tupla (votos, 1) que llego del Mapper
        for votos, un_acta in valores_tuplas:
            suma_total += votos  # Suma los votos
            cantidad_mesas += un_acta # Suma las actas (unos)

        promedio = suma_total / cantidad_mesas
        yield candidato, round(promedio, 2)
        # TERMINA TU CODIGO AQUI

if __name__ == '__main__':
    PromedioActas.run()
