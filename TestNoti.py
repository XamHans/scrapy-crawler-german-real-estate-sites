from pyfcm import FCMNotification
import traceback




class Notify:

    db = None
    myclient = None
    mongoDb = None
    immosCol = None
    countQuery = None

    push_service = FCMNotification(
        api_key="AAAAy9N4W0c:APA91bH62Vv_zkFSMLBjYIQ3fePeFRnixCErDySDHu8tmAIy8afBD-7c4CTWCwkiE82UOYlRTsCTKQ-Pf4Y1ItXVTPMflPMIn0Fenm-NgISP_eErZTiZ_HYp_nZUf2CB-D9WF2x5tgh_")



    def showBenachrichtung(self):
        try:

              

                    registration_id = 'ffTzCXVS_3Q:APA91bEUb_A1TiNreD7-uMWEyuW8NtL83U7rIVR9QVoamiIh3uFR4S9NeuOuRoYNy2-r0BhIbbg-MpSTZliORaUyCuInZ8VBLCB56Z8BLFn2zi1VXxu6JN2Jy8XKJiVVVDI0DCBy3jlj'
                    print('notify to: '+str(registration_id))
                    message_title = "ImmoRobo"
                    message_body = "Hi, hilf uns die App zu verbessern indem du die Feedback Funktion benutzt. Vielen Dank. "
                       
                    result = self.push_service.notify_single_device(
                        registration_id=registration_id, message_title=message_title, message_body=message_body)
                    print(str(result))



        except Exception as ex:
            traceback.print_exc()

notify = Notify()
notify.showBenachrichtung()