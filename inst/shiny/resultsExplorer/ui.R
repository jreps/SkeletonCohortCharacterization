shinydashboard::dashboardPage(
  shinydashboard::dashboardHeader(title = "Characteristics Explorer"),
  
  shinydashboard::dashboardSidebar(
    shinydashboard::sidebarMenu(
      id = "tabs",
      ## Tabs
      shinydashboard::menuItem("About", tabName = "about"),
      shinydashboard::menuItem("Results", tabName = "results")
      
    )
    
  ),
  
  shinydashboard::dashboardBody(
    
    tags$body(tags$div(id="ppitest", style="width:1in;visible:hidden;padding:0px")),
    tags$script('$(document).on("shiny:connected", function(e) {
                                    var w = window.innerWidth;
                                    var h = window.innerHeight;
                                    var d =  document.getElementById("ppitest").offsetWidth;
                                    var obj = {width: w, height: h, dpi: d};
                                    Shiny.onInputChange("pltChange", obj);
                                });
                                $(window).resize(function(e) {
                                    var w = $(this).width();
                                    var h = $(this).height();
                                    var d =  document.getElementById("ppitest").offsetWidth;
                                    var obj = {width: w, height: h, dpi: d};
                                    Shiny.onInputChange("pltChange", obj);
                                });
                            '),
    
    shinydashboard::tabItems(
      shinydashboard::tabItem(
        tabName = "about",
        shiny::br(),
        shiny::p(
          "This is an interactive viewer for exploring characteristic results."
        ),
        shiny::h3("[ADD HEADER]"),
        shiny::p(
          "[ADD TEXT]"
        )
        
      )
      ,
      
      shinydashboard::tabItem(tabName = "results",
                              
                              
                              shiny::fluidRow(width=12,
                                              shinydashboard::box(status = 'info', width = 12,
                                                                  title = "Table View", solidHeader = TRUE,
                                                                  DT::dataTableOutput('resultTable')))
                              
                              
                              
      )
      
    )
  )
)
