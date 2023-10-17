import unittest
from typing import Dict

from processor.fetcher import fetch_all_html

# random samples from our real database
sample_urls = [
    "https://www.oestafeta.com.br/seguranca/homem-e-morto-por-disparos-de-arma-de-fogo-em-bento-goncalves-2/",
    "https://www.otempo.com.br/cidades/policia-prende-12-pessoas-envolvidas-em-mais-de-10-assassinatos-na-grande-bh-1.2794897",
    "https://www.bbc.com/portuguese/geral-64107883?at_medium=RSS&at_campaign=KARANGA",
    "https://viapais.com.ar/streaming/se-conocieron-las-primeras-imagenes-de-la-pelicula-sobre-nahir-galarza/",
    "https://www.krqe.com/news/larry-barker/the-glitch-no-federal-background-checks-on-new-mexico-cannabis-applicants/",
    "https://www.topmidianews.com.br/interior/casal-surra-mulher-em-conveniencia-e-ainda-ameaca-com-faca-em-ladario/177371/",
    "https://www1.folha.uol.com.br/cotidiano/2023/07/o-barulho-dos-gritos-ficou-gravado-na-minha-mente-diz-sobrevivente-da-chacina-da-candelaria.shtml",
    "https://www.elsoldecordoba.com.mx/mexico/politica/en-movimiento-ciudadano-vamos-a-ir-solos-dante-delgado-10419832.html",
    "https://www.tachiranews.com/privadas-de-libertad-dos-mujeres-por-homicidio-de-dos-femeninas-en-arrollamiento-en-tachira/",
    "https://www.bonde.com.br/bondenews/policia/homem-que-matou-um-e-feriu-mais-tres-em-briga-por-causa-de-cigarro-e-preso-em-curitiba",
    "https://blogdacidadania.com.br/2023/01/bolsonaristas-ameacam-mulheres-de-esquerda/",
    "https://plazadearmas.com.mx/las-olvidadas-experiencias-presidenciales/",
    "https://www.acessa.com/mundo/2023/06/157682-violencia-na-franca-traz-a-memoria-revoltas-de-2005-e-dna-de-protestos.html",
    "https://elcomercio.pe/mundo/africa/mohamed-bazoum-denuncian-que-presidente-depuesto-en-niger-y-su-familia-carecen-de-agua-y-comida-fresca-niamey-abdourahamane-tiani-ultimas-noticia/",
    "https://www.montevideo.com.uy/auc.aspx?860328",
    "https://diarioelpueblo.com.uy/39-estafas-aclaradas-por-policia-de-lavalleja/",
    "https://www.cliquef5.com.br/geral/policia-mt/setimo-envolvido-em-homicidio-de-adolescente-e-apreendido-pela-policia-civil/326720",
    "https://www.teletica.com/bbc-news-mundo/5-momentos-de-la-tragica-vida-y-carrera-de-la-desafiante-sinead-oconnor_339434",
    "https://n.news.naver.com/mnews/article/421/0007038753?sid=103",
]


class TestUrlSpider(unittest.TestCase):
    def test_fetch_all_html(self):
        handled_count = 0

        def handle_parse(story: Dict):
            # called once for each URL in input
            nonlocal handled_count
            handled_count += 1
            assert 'original_url' in story
            assert 'story_text' in story

        fetch_all_html(sample_urls, handle_parse)


if __name__ == '__main__':
    unittest.main()
