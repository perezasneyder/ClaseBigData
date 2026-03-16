from mrjob.job import MRJob

class EscrutinioEficiente(MRJob):

    def mapper(self, _, linea):
        voto = linea.strip()
        if voto in ['Cepeda', 'Paloma', 'Fajardo', 'Abelardo']:
            yield voto, 1

    def combiner(self, candidato, conteos_locales):
        # INICIA TU CODIGO AQUI
        # La logica del Combiner para sumas algebraicas... ¡Es idéntica a la del reducer!
        # Suma los conteos locales y envialos (haz yield)
        yield candidato, sum(conteos_locales)
        # TERMINA TU CODIGO AQUI

    def reducer(self, candidato, conteos_totales):
        yield candidato, sum(conteos_totales)

if __name__ == '__main__':
    EscrutinioEficiente.run()
