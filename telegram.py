import requests
class Telegram:
        @staticmethod
        def send_message(immo):
             
                cap = """\
                <a href='{link}'><b>{title}</b></a>         
                &#8226;Gesamtmiete: {gesamtkosten}€
                &#8226;Gesamtfläche: {flache}qm²
                &#8226;Zimmer: {zimmer}   
                Mehrere Angebote findest du unter <a href='https://www.immorobo.de'>ImmoRobo.de</a>         
                """.format(title=immo['title'], link=immo['url'], gesamtkosten=immo['gesamtkosten'], flache=immo['flache'],
                           zimmer=immo['zimmer'] )
                
                token="xxx"
                url = f'https://api.telegram.org/bot{token}/sendPhoto'
                data = {'chat_id': {immo['chatid']},
                        'photo':immo['images'][0],
                        'parse_mode':'HTML',
                        'caption': cap}
                res = requests.post(url, data).json()
                #print(str(res))
