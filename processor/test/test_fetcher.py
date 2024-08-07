import unittest
from typing import Dict

from processor.fetcher import fetch_all_html, group_urls_by_domain

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

# Using another real sample url list, since there are no domain groupings in previous list
domain_sample_urls = [
    "http://collingwoodtoday.ca/world-news/911-transcripts-reveal-chaotic-scene-as-gunman-killed-18-people-in-maine-8072082",
    "http://collingwoodtoday.ca/world-news/police-say-17-year-old-killed-a-sixth-grader-and-wounded-five-in-iowa-school-shooting-8055737",
    "http://collingwoodtoday.ca/world-news/police-say-there-has-been-a-shooting-at-a-high-school-in-perry-iowa-extent-of-injuries-unclear-8055737",
    "http://colombotelegraph.com/index.php/who-accounts-for-the-law-keepers-when-they-become-the-law-breakers",
    "http://colonbuenosaires.com.ar/elfaro/primer-femicidio-del-ano-matan-a-golpes-a-una-joven-en-lomas-de-zamora-y-detienen-a-su-novio/",
    "http://colonbuenosaires.com.ar/elfaro/trenque-lauquen-conmocion-asesinaron-a-una-mujer-de-78-anos-y-detuvieron-a-la-hija/",
    "http://coloradocommunitymedia.com/stories/autopsies-in-franktown-area-double-homicide-show-other-injuries-drug-use,391806",
    "http://coloradocommunitymedia.com/stories/letters-to-the-editor,390458",
    "http://coloradocommunitymedia.com/stories/lifetimes-spring-movie-slate-features-saints-and-sinners,391795",
    "http://coloradocommunitymedia.com/stories/two-charged-in-brighton-fentanyl-death,391912",
    "http://columbustelegram.com/news/nation-world/crime-courts/texas-police-release-new-footage-in-murder-investigation"
    "-of-pregnant-woman-boyfriend/article_564a22d2-7df6-5eba-8910-3a83c6a536e0.html",
    "http://columbustelegram.com/news/nation-world/san-antonio-law-enforcement-crime-homicide-government-surveillance"
    "/article_3c9fa5c7-e6c1-56fe-bab8-4a6eb15e756a.html",
    "http://columbustelegram.com/news/state-regional/crime-courts/omaha-police-arrest-woman-in-connection-with-friday"
    "-homicide/article_55f70eee-a2bc-5909-b3e1-cdb0c26743c2.html",
]


class TestUrlSpider(unittest.TestCase):
    def test_fetch_all_html(self):
        handled_count = 0

        def handle_parse(story: Dict):
            # Called once for each URL in input
            nonlocal handled_count
            handled_count += 1
            assert "original_url" in story
            assert "story_text" in story

        fetch_all_html(sample_urls, handle_parse)

    def test_group_urls_by_domain(self):
        # Test Case 1 (w/ domain overlap)
        expected_output = [
            [
                "http://collingwoodtoday.ca/world-news/911-transcripts-reveal-chaotic-scene-as-gunman"
                "-killed-18-people-in-maine-8072082",
                "http://collingwoodtoday.ca/world-news/police-say-17-year-old-killed-a-sixth-grader-and"
                "-wounded-five-in-iowa-school-shooting-8055737",
                "http://collingwoodtoday.ca/world-news/police-say-there-has-been-a-shooting-at-a-high"
                "-school-in-perry-iowa-extent-of-injuries-unclear-8055737",
            ],
            [
                "http://colombotelegraph.com/index.php/who-accounts-for-the-law-keepers-when-they-become"
                "-the-law-breakers"
            ],
            [
                "http://colonbuenosaires.com.ar/elfaro/primer-femicidio-del-ano-matan-a-golpes-a-una"
                "-joven-en-lomas-de-zamora-y-detienen-a-su-novio/",
                "http://colonbuenosaires.com.ar/elfaro/trenque-lauquen-conmocion-asesinaron-a-una-mujer"
                "-de-78-anos-y-detuvieron-a-la-hija/",
            ],
            [
                "http://coloradocommunitymedia.com/stories/autopsies-in-franktown-area-double-homicide"
                "-show-other-injuries-drug-use,391806",
                "http://coloradocommunitymedia.com/stories/letters-to-the-editor,390458",
                "http://coloradocommunitymedia.com/stories/lifetimes-spring-movie-slate-features-saints"
                "-and-sinners,391795",
                "http://coloradocommunitymedia.com/stories/two-charged-in-brighton-fentanyl-death,"
                "391912",
            ],
            [
                "http://columbustelegram.com/news/nation-world/crime-courts/texas-police"
                "-release-new-footage-in-murder-investigation-of-pregnant-woman-boyfriend"
                "/article_564a22d2-7df6-5eba-8910-3a83c6a536e0.html",
                "http://columbustelegram.com/news/nation-world/san-antonio-law-enforcement"
                "-crime-homicide-government-surveillance/article_3c9fa5c7-e6c1-56fe-bab8"
                "-4a6eb15e756a.html",
                "http://columbustelegram.com/news/state-regional/crime-courts/omaha-police"
                "-arrest-woman-in-connection-with-friday-homicide/article_55f70eee-a2bc-5909"
                "-b3e1-cdb0c26743c2.html",
            ],
        ]

        # perform the URL grouping
        domain_list = group_urls_by_domain(domain_sample_urls)

        # sort the lists within domain_list for consistent comparison
        domain_list_sorted = [sorted(group) for group in domain_list]
        expected_output_sorted = [sorted(group) for group in expected_output]

        # check the groupings are correct
        self.assertCountEqual(
            domain_list_sorted, expected_output_sorted, "URL grouping by domain failed"
        )

        # Test Case 2 (w/ unique domains)
        expected_output = [[url] for url in sample_urls]
        grouped_urls = group_urls_by_domain(sample_urls)
        self.assertEqual(grouped_urls, expected_output)


if __name__ == "__main__":
    unittest.main()
