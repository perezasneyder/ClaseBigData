from mrjob.job import MRJob

class VarianzaActas(MRJob):

    def mapper(self, _, linea):
        partes = linea.strip().split(',')
        if len(partes) == 3:
            cand = partes[0]
            x = float(partes[2])

            # INICIA TU CODIGO AQUI
            # Emite una tupla con 3 valores: (El numero 1, el valor original 'x', y el valor al cuadrado)
            yield cand, (1, x, x**2)
            # TERMINA TU CODIGO AQUI

    def reducer(self, candidato, tuplas):
        n = 0
        suma_x = 0
        suma_x2 = 0

        for conteo, x, x2 in tuplas:
            n += conteo
            suma_x += x
            suma_x2 += x2

        # INICIA TU CODIGO AQUI
        if n > 0:
            promedio = suma_x / n
            # Implementa la formula matematica descrita en el markdown usando las variables ya acumuladas arriba
            varianza = (suma_x / n) - (promedio ** 2)
            yield candidato, round(varianza, 2)
        # TERMINA TU CODIGO AQUI

if __name__ == '__main__':
    VarianzaActas.run()
