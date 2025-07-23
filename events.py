from pupa.scrape import Scraper
from pupa.scrape import Event
from pupa.scrape import Jurisdiction

class Ocd4Oak(Jurisdiction):

    division_id = "ocd-division/country:us/state:ca/place:oakland"
    name = " City of Oakland, CA"
    url = " https://www.oaklandca.gov"
    scrapers = {
        "events": Ocd4OakEventScraper,
    }

class Ocd4OakEventScraper(Scraper):

    def scrape(self):
        when = dt.datetime(1776,7,4,9,15)
        tz = pytz.timezone("US/Pacific") #set the timezone for this location
        when = tz.localize(when)
        e = Event(name="Hearing",  # Event Name
                      start_time=when,  # When the event will take place
                      timezone=tz.zone, #the local timezone for the event
                      location_name='Town Hall')  # Where the event will be
        e.add_source("http://example.com")

        #add a committee
        e.add_participant(name="Transportation Committee",
                        type="committee")

        #add a person
        e.add_person(name="Joe Smith", note="Hearing Chair")

        #add an mpeg video
        e.add_media_link(note="Video of meeting",
                        url="http://example.com/hearing/video.mpg",
                        media_type="video/mpeg")

        #add a pdf of meeting minutes
        e.add_media_link(note="Meeting minutes",
                        url="http://example.com/hearing/minutes.pdf",
                        media_type="application/pdf")

        #add an agenda item to this event
        a = e.add_agenda_item(description="Testimony from concerned citizens")

        #the testimony is about transportation and the environment
        a.add_subject("Transportation")
        a.add_subject("Environment")

        #and includes these two committees
        a.add_committee("Transportation")
        a.add_committee("Environment and Natural Resources")

        #these people will be present
        a.add_person("Jane Brown")
        a.add_person("Alicia Jones")
        a.add_person("Fred Green")

        #they'll be discussing this bill
        a.add_bill("HB101")

        #here's a document that is included
        a.add_media_link(note="Written version of testimony",
                        url="http://example.com/hearing/testimony.pdf",
                        media_type="application/pdf")

        yield e
