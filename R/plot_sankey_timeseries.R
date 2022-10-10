#'  plot_sankey_timeseries
#'
#' @param timeseries_df f
#' @param plot_title f
#'
#' @importFrom rlang sym
#' @importFrom plotly plot_ly
#' @importFrom grDevices palette rainbow
#'
#' @return f
#' @export
#'
plot_sankey_timeseries <- function(timeseries_df, plot_title){
  # Input checks
  stopifnot(
    "The cohort_df must be a data frame" = is.data.frame(timeseries_df)
    #,
    # colnames must be ..... id, time_step, group
  )

  # Start generalised Sankey processing
  links = data.frame(source = as.character(),
                     target = as.character(),
                     value  = as.numeric())

  timestep = unique(timeseries_df$time_step)

  for(i in seq_along(timestep)){

    if(i==1){

      l = timeseries_df %>%
        dplyr::filter   (.data$time_step == timestep[i]) %>%
        dplyr::group_by (.data$group) %>%
        dplyr::summarise(source = "cohort",
                         value  = dplyr::n()) %>%
        dplyr::mutate(target = paste(.data$group, timestep[i], sep="_")) %>%
        dplyr::relocate(.data$target, .before = .data$value)
      links = rbind(links, l)

    }else{

      l = dplyr::full_join(timeseries_df %>% dplyr::filter(.data$time_step == timestep[i-1]),
                           timeseries_df %>% dplyr::filter(.data$time_step == timestep[i]),
                           by = c("id"),
                           suffix = as.character(c(timestep[i-1], timestep[i]))) %>%
        tidyr::drop_na() %>%
        dplyr::group_by(group = !!rlang::sym(paste0("group", timestep[i-1])), !!rlang::sym(paste0("group", timestep[i]))) %>%
        dplyr::summarise(value  = dplyr::n()) %>%
        dplyr::mutate(source = paste(.data$group, timestep[i-1], sep="_"),
                      target = paste(!!rlang::sym(paste0("group", timestep[i])), timestep[i], sep="_")) %>%
        dplyr::select(.data$group,
                      .data$source,
                      .data$target,
                      .data$value)
      links = rbind(links, l)
    }
  }


  links = links %>%
          dplyr::mutate(source = factor(.data$source, levels=unique(c(.data$source, .data$target))),
                        target = factor(.data$target, levels=unique(c(.data$source, .data$target))))
  node_df = data.frame(name = unique(c(links$source, links$target))) %>%
            dplyr::mutate(group = sub("_.*", "", .data$name))
  # Set up colours
  unique_groups = unique(node_df$group)
  grDevices::palette(rainbow(length(unique_groups)))
  colour_df     = data.frame(group = unique_groups, colour = grDevices::palette())
  node_df = dplyr::left_join(node_df, colour_df, by=c("group"))

  # Convert source-target matches to numeric
  links$IDsource <- match(links$source, unique(c(links$source, links$target)))-1
  links$IDtarget <- match(links$target, unique(c(links$source, links$target)))-1

  fig <- plotly::plot_ly(
    type        = "sankey",
    domain      = list(x = c(0,1), y = c(0,1)),
    orientation = "h",
    valueformat = ".0f",
    valuesuffix = "TWh",
    node        = list(label     = node_df$name,
                       color     = node_df$colour,
                       pad       = 15,
                       thickness = 15,
                       line      = list(color = "black", width = 0.5)),
    link        = list(source    = links$IDsource,
                       target    = links$IDtarget,
                       value     = links$value,
                       label     = rep("foo", 3))
  )

  fig <- fig %>% plotly::layout(
    title = plot_title,
    font  = list(size = 10),
    xaxis = list(showgrid = F, zeroline = F),
    yaxis = list(showgrid = F, zeroline = F)
  )

  return(fig)
}
