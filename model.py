class CondoMesage:
    def __init__(self, image_url, title, text, link):
        self.image_url = image_url
        self.title = title
        self.text = text
        self.link = link

    def serialize(self):
        return {
            "image_url": self.image_url,
            "title": self.title,
            "text": self.text,
            "link": self.link
        }
