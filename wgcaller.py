from pywebpush import webpush


webpush(
    subscription_info={
        "endpoint": "https://fcm.googleapis.com/fcm/send/ehHrKREU46U:APA91bGqgMal1tgsPJ7KApCkAmdx6KHRnW8DlNhO_P885wKkbI2spR5yaEBTebtucrQ-FMi60BQ5ECfuRtMajOCGZqCTmHnau8WPX9g-CnlwZrqW0gjazdl1OY80eq3fbPOO735qqbKN",
        "keys": {
            "p256dh": "BInz4nHAnVlIKGNKmwCx1IRZdzbX5asW1L9zRKI4wPd1avBoACNizcg3ig3uLzsZWxGeHjbWi_SlEGz83Cz1LYE",
            "auth": "GH6N-U7PD83hD4-k3iGt8g"
        }},
    data="Es gibt neue Anzeigen für dich",
    vapid_private_key="ExvBt2T-Z20hZaPwn1GjrRtkKs6db0OvKKyks94ES_k",
    vapid_claims={
            "sub": "mailto:muellerjohannes93@gmail.com",
        }
)